import math
import os
import subprocess
import tempfile
import sys
import re


class SizedInt:
    UNIT_SCALE = {"B": 0, "K": 1, "M": 2, "G": 3, "T": 4}
    SCALE_UNIT = ["B", "K", "M", "G", "T"]
    BASE = 1024

    def __init__(self, value, unit="B"):
        if unit not in self.UNIT_SCALE:
            raise ValueError(f"Invalid unit: {unit}")
        self.value_in_bytes = float(value) * (self.BASE ** self.UNIT_SCALE[unit])

    @classmethod
    def from_bytes(cls, num_bytes: int) -> "SizedInt":
        return cls(num_bytes, "B")

    @classmethod
    def from_str(cls, s):
        """
        Construct a SizedInt object from a string like '4M' or '2.5G'.
        Also handles case where input is already a SizedInt.
        """
        if isinstance(s, cls):
            return s

        s = str(s).strip().upper()

        # Handle raw integers (bytes) passed as string
        if s.isdigit():
            return cls(float(s), "B")

        match = re.fullmatch(r"([\d.]+)([BKMGTP])", s)
        if not match:
            raise ValueError(
                f"Invalid format: {s}. Expected format like '4K', '2.5G' or raw bytes"
            )
        val, unit = match.groups()
        return cls(float(val), unit)

    def __repr__(self):
        return self._format()

    def _format(self):
        val = self.value_in_bytes
        ui = 0
        while val >= self.BASE and ui < len(self.SCALE_UNIT) - 1:
            tmp = val / self.BASE
            if not tmp.is_integer():
                break
            val = tmp
            ui += 1
        return f"{int(val)}{self.SCALE_UNIT[ui]}"

    def to_bytes(self):
        return int(self.value_in_bytes)

    def to(self, unit):
        return self.value_in_bytes / (self.BASE ** self.UNIT_SCALE[unit])


def prompt_input(
    prompt: str, default=None, convert_fn=str, type_name: str = None
) -> any:
    """
    General-purpose input reader with default value and type conversion.

    Args:
        prompt (str): Prompt text shown to user.
        default: The default value (optional).
        convert_fn: Conversion function, e.g., int, float, SizedInt.from_str.
        type_name (str): Optional name to show for the type (e.g. "integer", "float").

    Returns:
        The converted value, or default if input is empty.
    """
    if type_name is None:
        type_name = convert_fn.__name__ if hasattr(convert_fn, "__name__") else "value"

    full_prompt = f"{prompt}"
    if default is not None:
        full_prompt += f" [default: {default}]"
    full_prompt += ": "

    while True:
        user_input = input(full_prompt).strip()
        if not user_input:
            if default is not None:
                return convert_fn(default)
            print("No input provided and no default value. Please try again.")
            continue
        try:
            return convert_fn(user_input)
        except Exception as e:
            print(f"Invalid {type_name}: {e}. Please try again.")


def ceildiv(a: SizedInt, b) -> SizedInt:
    """
    Compute ceil(a / b) where a and b are SizeInt instances.

    Returns:
        int: The smallest integer ≥ a / b
    """
    a_bytes = int(a.to_bytes())
    if isinstance(b, int):
        b_bytes = b
    elif isinstance(b, SizedInt):
        b_bytes = int(b.to_bytes())
    else:
        raise TypeError(f"Expected int or SizedInt, got {type(b).__name__}")

    if b_bytes == 0:
        raise ZeroDivisionError("Division by zero not allowed.")
    return math.ceil(a_bytes / b_bytes)


class SSDConfigManager:
    def __init__(self, filepath, prototype):
        self.filepath = filepath
        self.prototype = prototype
        self.lines = []
        self.start_idx = -1
        self.end_idx = -1
        self.macros = {}  # Cache for macros in the active block
        self._load_and_locate_block()

    def _load_and_locate_block(self):
        if not os.path.exists(self.filepath):
            raise FileNotFoundError(f"{self.filepath} not found")

        with open(self.filepath, "r", encoding="utf-8") as f:
            self.lines = f.readlines()

        # Locate the block for the specific prototype
        target_def = f"BASE_SSD == {self.prototype.upper()}_PROTOTYPE"

        in_block = False
        depth = 0

        for i, line in enumerate(self.lines):
            if "#elif" in line or "#if" in line:
                if target_def in line:
                    self.start_idx = i
                    in_block = True
                    depth = 1
                elif in_block and ("#elif" in line or "#else" in line) and depth == 1:
                    self.end_idx = i
                    break

            if in_block:
                if "#if" in line:
                    depth += 1
                elif "#endif" in line:
                    depth -= 1
                    if depth == 0:
                        self.end_idx = i
                        break

        if self.start_idx == -1:
            raise ValueError(f"Could not find configuration block for {self.prototype}")
        if self.end_idx == -1:
            self.end_idx = len(self.lines)

    def _parse_block_defines(self):
        """Scans the active block and populates self.macros"""
        self.macros = {}
        # Regex to capture #define NAME VALUE (ignoring comments)
        pat = re.compile(r"^\s*#define\s+(\w+)\s+(.*)$")

        for i in range(self.start_idx, self.end_idx):
            line = self.lines[i].strip()
            # Remove comments // and /* ... */ (simple assumption: single line)
            if "//" in line:
                line = line.split("//")[0]
            if "/*" in line:
                line = line.split("/*")[0]

            match = pat.match(line)
            if match:
                key = match.group(1)
                val = match.group(2).strip()
                self.macros[key] = val

    def _resolve_recursive(self, val_str, stack=None):
        """Recursively expands macros in val_str using self.macros"""
        if stack is None:
            stack = []

        # Clean up suffixes like 66ULL -> 66
        val_str = re.sub(r"(\d+)[Uu][Ll][Ll]", r"\1", val_str)
        val_str = re.sub(r"(\d+)[Uu][Ll]", r"\1", val_str)
        val_str = re.sub(r"(\d+)[Uu]", r"\1", val_str)

        # Expand MB/GB/KB macros to math expressions
        val_str = re.sub(r"MB\s*\(\s*(.*?)\s*\)", r"(\1 * 1048576)", val_str)
        val_str = re.sub(r"GB\s*\(\s*(.*?)\s*\)", r"(\1 * 1073741824)", val_str)
        val_str = re.sub(r"KB\s*\(\s*(.*?)\s*\)", r"(\1 * 1024)", val_str)

        # Find potential identifiers (uppercase words starting with letter/underscore)
        tokens = re.findall(r"\b[A-Z_][A-Z0-9_]*\b", val_str)

        for token in tokens:
            if token in self.macros:
                if token in stack:
                    # Circular dependency detected, abort recursion for this token
                    continue

                # Recursively resolve the token
                expanded = self._resolve_recursive(self.macros[token], stack + [token])

                # Replace whole word matches only
                val_str = re.sub(
                    r"\b" + re.escape(token) + r"\b", f"({expanded})", val_str
                )

        return val_str

    def get_define_eval(self, key):
        """
        Parses defines, expands macros recursively, and evals the math.
        """
        self._parse_block_defines()  # Always refresh before read

        if key not in self.macros:
            return None

        raw_val = self.macros[key]
        expanded_expression = self._resolve_recursive(raw_val, [key])

        try:
            # Safe eval
            return int(eval(expanded_expression, {"__builtins__": None}, {}))
        except Exception as e:
            print(f"[Warn] Failed to eval macro '{key}': {expanded_expression} -> {e}")
            return 0

    def get_define(self, key):
        self._parse_block_defines()
        return self.macros.get(key)

    def set_define(self, key, new_val_str, description=None):
        """Updates #define KEY with new_val_str if confirmed by user"""
        # Scan lines directly to find the line index
        pattern = re.compile(r"^(\s*#define\s+" + re.escape(key) + r"\s+)(.*)$")
        found_line_idx = -1
        current_val = ""

        for i in range(self.start_idx, self.end_idx):
            match = pattern.match(self.lines[i])
            if match:
                found_line_idx = i
                # Strip comments for comparison
                val_part = match.group(2)
                if "//" in val_part:
                    current_val = val_part.split("//")[0].strip()
                elif "/*" in val_part:
                    current_val = val_part.split("/*")[0].strip()
                else:
                    current_val = val_part.strip()
                break

        if found_line_idx != -1:
            if current_val == new_val_str:
                return  # No change needed

            print(f"\n[Config Change] Need to update '{key}' in ssd_config.h")
            print(f"  Current: {current_val}")
            print(f"  New:     {new_val_str}")
            if description:
                print(f"  Reason:  {description}")

            confirm = input("  >> Confirm overwrite? (y/n) [n]: ").strip().lower()
            if confirm in ("y", "yes"):
                match = pattern.match(self.lines[found_line_idx])
                prefix = match.group(1)

                # Preserve existing comments
                original_line = self.lines[found_line_idx]
                comment = ""
                if "//" in original_line:
                    comment = " //" + original_line.split("//", 1)[1].strip()
                elif "/*" in original_line:
                    comment = " /*" + original_line.split("/*", 1)[1].strip()

                self.lines[found_line_idx] = f"{prefix}{new_val_str}{comment}\n"
                self._save()
                print("  Updated.")
            else:
                print("  Skipped.")
        else:
            print(f"[Warn] Could not find #define {key} to update.")

    def _save(self):
        with open(self.filepath, "w", encoding="utf-8") as f:
            f.writelines(self.lines)

    def format_size_macro(self, bytes_val):
        """Converts bytes to MB() or GB() format if clean, else bytes"""
        if bytes_val == 0:
            return "(0)"
        if bytes_val % (1024**3) == 0:
            return f"GB({bytes_val // (1024**3)}ULL)"
        elif bytes_val % (1024**2) == 0:
            return f"MB({bytes_val // (1024**2)}ULL)"
        elif bytes_val % 1024 == 0:
            return f"KB({bytes_val // 1024}ULL)"
        return f"({bytes_val}ULL)"


def get_f2fs_constants_via_gcc(header_path):
    """
    Compiles a tiny C program to print the exact values of SIT_ENTRY_PER_BLOCK
    , NAT_ENTRY_PER_BLOCK and so on from the given header file.
    This handles 'sizeof' and math expressions that regex cannot parse.
    """
    # 1.
    header_path = os.path.abspath(header_path)
    include_dir = os.path.dirname(header_path)
    header_name = os.path.basename(header_path)

    # 2. a micro C program
    # Note：we need to define _LARGEFILE64_SOURCE
    c_source = f"""
    #define _LARGEFILE64_SOURCE
    #include <stdio.h>
    #include <stddef.h>
    
    #include "{header_name}"

    int main() {{
        printf("%lu %lu %lu %lu", (unsigned long)SIT_ENTRY_PER_BLOCK, (unsigned long)NAT_ENTRY_PER_BLOCK,  (unsigned long)MAX_BITMAP_SIZE_IN_CKPT, (unsigned long)MAX_SIT_BITMAP_SIZE_IN_CKPT);
        return 0;
    }}
    """

    # 3.
    with tempfile.TemporaryDirectory() as temp_dir:
        src_file = os.path.join(temp_dir, "probe.c")
        bin_file = os.path.join(temp_dir, "probe.out")

        with open(src_file, "w") as f:
            f.write(c_source)

        try:
            cmd = ["gcc", "-I", include_dir, src_file, "-o", bin_file]

            # 如果还有其他依赖目录（例如 android_config.h），可以在这里添加更多 -I
            # cmd.extend(["-I", "/path/to/other/includes"])

            subprocess.check_output(cmd, stderr=subprocess.STDOUT)

            # 4. get results
            output = subprocess.check_output([bin_file]).decode().strip()
            val_sit, val_nat, val_max_ckpt, val_max_sit_ckpt = map(int, output.split())
            return val_sit, val_nat, val_max_ckpt, val_max_sit_ckpt

        except subprocess.CalledProcessError as e:
            print(
                f"[Warning] GCC probe failed. Compilation output:\n{e.output.decode()}"
            )
            raise RuntimeError("Could not compile C probe to determine constants.")


def calculate_f2fs_meta_size(
    data_size_bytes,
    zone_size_bytes,
    block_size_bytes=4096,
):
    """
    Simulates f2fs_format.c logic accurately, including NAT clamping based on
    Checkpoint (CP) bitmap limitations.

    Ref: f2fs_format.c lines 316-340
    """

    # --- Constants from F2FS Headers ---
    F2FS_BLKSIZE = 4096  # 4KB
    F2FS_MAX_SEGMENT = (16 * 1024 * 1024) // 2
    SEG_SIZE_BYTES = 2 * 1024 * 1024  # 2MB
    BLKS_PER_SEG = SEG_SIZE_BYTES // block_size_bytes  # 512
    CKPT_SEGS = 2
    segs_per_zone = zone_size_bytes // SEG_SIZE_BYTES

    F2FS_HEADER_PATH = "./f2fs-tools-1.14.0/include/f2fs_fs.h"

    SIT_ENTRY_PER_BLOCK = 55
    NAT_ENTRY_PER_BLOCK = 455
    MAX_BITMAP_SIZE_IN_CKPT = 3900
    MAX_SIT_BITMAP_SIZE_IN_CKPT = 3836

    try:
        print(f"Reading constants from {F2FS_HEADER_PATH}...")
        (
            SIT_ENTRY_PER_BLOCK,
            NAT_ENTRY_PER_BLOCK,
            MAX_BITMAP_SIZE_IN_CKPT,
            MAX_SIT_BITMAP_SIZE_IN_CKPT,
        ) = get_f2fs_constants_via_gcc(F2FS_HEADER_PATH)
        print(
            f"Found C definitions: SIT_ENTRY={SIT_ENTRY_PER_BLOCK}, NAT_ENTRY={NAT_ENTRY_PER_BLOCK}, MAX_BITMAP_SIZE_IN_CKPT={MAX_BITMAP_SIZE_IN_CKPT}, MAX_SIT_BITMAP_SIZE_IN_CKPT={MAX_SIT_BITMAP_SIZE_IN_CKPT}"
        )
    except Exception as e:
        print(f"[Warning] Failed to parse C header: {e}")
        print("Falling back to default values.")
        SIT_ENTRY_PER_BLOCK = 55
        NAT_ENTRY_PER_BLOCK = 455
        MAX_BITMAP_SIZE_IN_CKPT = 3900
        MAX_SIT_BITMAP_SIZE_IN_CKPT = 3836

    MAX_SIT_BITMAP_SIZE = (
        (
            (
                ((F2FS_MAX_SEGMENT + SIT_ENTRY_PER_BLOCK - 1) // SIT_ENTRY_PER_BLOCK)
                + BLKS_PER_SEG
                - 1
            )
            // BLKS_PER_SEG
        )
        * BLKS_PER_SEG
        // 8
    )
    # --- 1. Calculate Start Offset (segment0_blkaddr)
    # f2fs_format.c: zone_align_start_offset calculation
    # Assuming start_sector = 0 for image file.
    # Offset aligns the start to a zone boundary + 2 blocks (Superblock).
    start_offset_bytes = (
        (0 + 2 * F2FS_BLKSIZE + zone_size_bytes - 1) // zone_size_bytes
    ) * zone_size_bytes
    offset_segments = start_offset_bytes // SEG_SIZE_BYTES

    # --- 2. Iterative Calculation ---
    # Start iteration
    data_segments = data_size_bytes // SEG_SIZE_BYTES
    current_meta_segments = 0
    prev_meta_segments = -1
    # Loop to converge on exact segment count
    while current_meta_segments != prev_meta_segments:
        prev_meta_segments = current_meta_segments

        # 1. Total Segments (Raw Sum first)
        raw_total_segments = data_segments + offset_segments + current_meta_segments

        # F2FS aligns the *Total* volume size to Zone boundaries (Line 245)
        # segment_count = (c.total_segments / c.segs_per_zone * c.segs_per_zone)
        segment_count = (raw_total_segments // segs_per_zone) * segs_per_zone

        # 2. SIT Calculation
        blocks_for_sit = (
            segment_count + SIT_ENTRY_PER_BLOCK - 1
        ) // SIT_ENTRY_PER_BLOCK
        sit_segments = (
            blocks_for_sit * block_size_bytes + SEG_SIZE_BYTES - 1
        ) // SEG_SIZE_BYTES
        segment_count_sit = sit_segments * 2

        # 3. NAT Calculation with CLAMPING (The Key Step)
        # First, Calculate theoretical needs
        valid_blks_for_nat = (
            segment_count - (CKPT_SEGS + segment_count_sit)
        ) * BLKS_PER_SEG
        if valid_blks_for_nat < 0:
            valid_blks_for_nat = 0

        blocks_for_nat = (
            valid_blks_for_nat + NAT_ENTRY_PER_BLOCK - 1
        ) // NAT_ENTRY_PER_BLOCK
        # assume c.large_nat_bitmap == 0
        nat_segments_raw = (
            blocks_for_nat * block_size_bytes + SEG_SIZE_BYTES - 1
        ) // SEG_SIZE_BYTES
        max_nat_bitmap_size = 0

        # 4. Bitmap Size Calculation
        # sit_bitmap_size = ((get_sb(segment_count_sit) / 2) << log_blks_per_seg) / 8;
        # log_blks_per_seg for 2MB segment is 9 (2^9 = 512)
        sit_bitmap_size = (sit_segments * BLKS_PER_SEG) // 8

        if sit_bitmap_size > MAX_SIT_BITMAP_SIZE:
            max_sit_bitmap_size = MAX_SIT_BITMAP_SIZE
        else:
            max_sit_bitmap_size = sit_bitmap_size

        if max_sit_bitmap_size > MAX_SIT_BITMAP_SIZE_IN_CKPT:
            max_nat_bitmap_size = MAX_BITMAP_SIZE_IN_CKPT
        else:
            max_nat_bitmap_size = MAX_BITMAP_SIZE_IN_CKPT - max_sit_bitmap_size
        max_nat_segments = (max_nat_bitmap_size * 8) // BLKS_PER_SEG

        # Clamp NAT size (Line 337)
        if nat_segments_raw > max_nat_segments:
            final_nat_segments = max_nat_segments
        else:
            final_nat_segments = nat_segments_raw

        segment_count_nat = final_nat_segments * 2

        # 5. SSA Calculation (Line 342-349)
        # SSA needs to cover segments that are NOT CP, SIT, or NAT
        valid_blks_for_ssa = (
            segment_count - (CKPT_SEGS + segment_count_sit + segment_count_nat)
        ) * BLKS_PER_SEG
        if valid_blks_for_ssa < 0:
            valid_blks_for_ssa = 0

        # blocks_for_ssa = total_valid_blks_available / c.blks_per_seg + 1
        blocks_for_ssa = (valid_blks_for_ssa // BLKS_PER_SEG) + 1
        segment_count_ssa = (
            blocks_for_ssa * block_size_bytes + SEG_SIZE_BYTES - 1
        ) // SEG_SIZE_BYTES

        # 6. Total Raw Meta
        total_meta_segments = (
            CKPT_SEGS + segment_count_sit + segment_count_nat + segment_count_ssa
        )

        # 7. Padding for Alignment (Line 354)
        # The meta area must end exactly at a zone boundary so Data starts fresh.
        # F2FS does this by extending the SSA area.
        diff = total_meta_segments % segs_per_zone
        if diff != 0:
            padding = segs_per_zone - diff
            segment_count_ssa += padding
            total_meta_segments += padding

        current_meta_segments = total_meta_segments

    return start_offset_bytes + current_meta_segments * SEG_SIZE_BYTES


def get_memmap_start_from_grub(grub_path="/etc/default/grub", default_val="102G"):
    """
    Parses /etc/default/grub to find the memmap start address.
    Looks for pattern like: memmap=60G\$102G or memmap=60G$102G
    Returns the start address (e.g., "102G") if found, otherwise returns default_val.
    """
    if not os.path.exists(grub_path):
        return default_val
    try:
        with open(grub_path, "r", encoding="utf-8") as f:
            content = f.read()
        pattern = re.compile(r"memmap=[^ ]+?\\?\$([0-9]+[KMGTP])", re.IGNORECASE)
        match = pattern.search(content)
        if match:
            found_val = match.group(1)
            print(f"[Info] Auto-detected memmap start from grub: {found_val}")
            return found_val
    except Exception as e:
        print(f"[Warning] Failed to read grub config: {e}")
    return default_val


if __name__ == "__main__":
    CONFIG_PATH = "./ssd_config.h"

    print("=== Emulator Configurator ===")

    # 0. Prototype Selection
    prototype = prompt_input(
        "Select prototype (conzone/zns)", default="conzone", convert_fn=str.lower
    )
    if prototype not in ["conzone", "zns"]:
        print("Invalid prototype, defaulting to conzone")
        prototype = "conzone"

    try:
        cfg = SSDConfigManager(CONFIG_PATH, prototype)
    except Exception as e:
        print(f"Error loading config: {e}")
        sys.exit(1)

    # 1. Memmap Start
    default_memmapstart = get_memmap_start_from_grub()
    memmap_start = prompt_input("Memmap start address", default=default_memmapstart)

    # 2. Flash Type (CELL_MODE)
    # Read from header
    curr_cell_mode_raw = cfg.get_define("CELL_MODE")
    default_flash_type = "TLC"
    if "CELL_MODE_TLC" in curr_cell_mode_raw:
        default_flash_type = "TLC"
    elif "CELL_MODE_QLC" in curr_cell_mode_raw:
        default_flash_type = "QLC"

    flash_type = prompt_input(
        "Flash type (TLC/QLC)", default=default_flash_type, convert_fn=str.upper
    )

    # Update header if needed
    target_cell_mode = f"CELL_MODE_{flash_type}"
    if target_cell_mode not in curr_cell_mode_raw:
        cfg.set_define(
            "CELL_MODE", f"({target_cell_mode})", f"User selected {flash_type}"
        )

    # 3. Interface Type
    # Read from header
    curr_ns_type = cfg.get_define("NS_SSD_TYPE_1")
    default_interface = "zoned"
    if "CONZONE_BLOCK" in curr_ns_type or "CONV" in curr_ns_type:
        default_interface = "block"

    interface_type = prompt_input(
        "Interface type (block/zoned)", default=default_interface, convert_fn=str.lower
    )

    # Update header if needed
    if interface_type == "zoned":
        new_val = "SSD_TYPE_CONZONE_ZONED" if prototype == "conzone" else "SSD_TYPE_ZNS"
    else:
        new_val = (
            "SSD_TYPE_CONZONE_BLOCK" if prototype == "conzone" else "SSD_TYPE_CONV"
        )

    if new_val not in curr_ns_type:
        cfg.set_define("NS_SSD_TYPE_1", new_val, f"User selected {interface_type}")
        # Assuming NS 0 follows suit or is defined statically in header logic, usually we just set NS 1 for tests

    # 4. Block Size
    # Read bytes from header
    curr_blk_size = cfg.get_define_eval("BLK_SIZE")
    default_bs = (
        SizedInt.from_bytes(curr_blk_size) if curr_blk_size else SizedInt(24, "M")
    )

    block_size = prompt_input(
        "Block size", default=default_bs, convert_fn=SizedInt.from_str, type_name="size"
    )

    if block_size.to_bytes() != curr_blk_size:
        # Convert to MB() format if possible
        new_macro = cfg.format_size_macro(block_size.to_bytes())
        cfg.set_define("BLK_SIZE", new_macro, f"User selected {block_size}")

    # 5. Planes per Superblock (Dies)
    # Read components
    n_dies = cfg.get_define_eval("DIES_PER_ZONE")
    n_pln = cfg.get_define_eval("PLNS_PER_LUN")
    curr_planes_per_sblk = n_dies * n_pln

    planes_per_sblk = prompt_input(
        "Planes per Superblock (Parallelism)",
        default=curr_planes_per_sblk,
        convert_fn=int,
    )

    if planes_per_sblk != curr_planes_per_sblk:
        print(
            f"  [Input Required] Target parallelism is {planes_per_sblk}, but config is {curr_planes_per_sblk} ({n_dies}*{n_pln})"
        )
        print("  Please define new geometry:")
        new_dies = prompt_input("    DIES_PER_ZONE", default=n_dies, convert_fn=int)
        new_pln = prompt_input("    PLNS_PER_LUN", default=n_pln, convert_fn=int)

        if new_dies * new_pln != planes_per_sblk:
            print(
                f"  [Warn] {new_dies}*{new_pln} = {new_dies*new_pln}, not {planes_per_sblk}. Using calculated value."
            )
            planes_per_sblk = new_dies * new_pln

        cfg.set_define("DIES_PER_ZONE", str(new_dies))
        cfg.set_define("PLNS_PER_LUN", str(new_pln))

    sblk_size_bytes = block_size.to_bytes() * planes_per_sblk

    # 6. pSLC Superblocks
    curr_pslc = cfg.get_define_eval("DATA_pSLC_INIT_BLKS")
    npslc_sblks = prompt_input(
        "pSLC superblocks for data", default=curr_pslc, convert_fn=int
    )

    if npslc_sblks != curr_pslc:
        cfg.set_define("DATA_pSLC_INIT_BLKS", f"({npslc_sblks})")

    # 7. Data Namespace Size
    logical_data_size = prompt_input(
        "Data namespace size",
        default="32G",
        convert_fn=SizedInt.from_str,
        type_name="size",
    )

    # 8. Meta Namespace Size
    default_meta = "0K"  # Trigger calc by default
    logical_meta_size = prompt_input(
        "Meta namespace size",
        default=default_meta,
        convert_fn=SizedInt.from_str,
        type_name="size",
    )

    zone_size_bytes = 0
    # Calc Logic
    if logical_meta_size.to_bytes() == 0:
        print(f"--- Auto-calculating Meta Namespace Size ---")
        # Read ZONE_SIZE from header

        if interface_type == "zoned":
            header_zone_size = cfg.get_define_eval("ZONE_SIZE")
            if not header_zone_size:
                header_zone_size = 2 * 1024 * 1024  # Fallback 2M
        else:
            header_zone_size = 2 * 1024 * 1024

        # Check power of 2
        if (header_zone_size & (header_zone_size - 1)) != 0:
            # Not power of 2, find next pow2
            pow2_zone = 2 ** math.ceil(math.log2(header_zone_size))
            print(
                f"  [Info] Configured ZONE_SIZE {SizedInt.from_bytes(header_zone_size)} is not power of 2."
            )
            print(f"         Using {SizedInt.from_bytes(pow2_zone)} for calculation.")
            zone_size_bytes = pow2_zone
        else:
            zone_size_bytes = header_zone_size

        print(f"  [Info] Using Zone Size: {SizedInt.from_bytes(zone_size_bytes)}")

        calc_meta_bytes = calculate_f2fs_meta_size(
            int(logical_data_size.to_bytes()), zone_size_bytes
        )
        logical_meta_size = SizedInt.from_bytes(calc_meta_bytes)
        print(f"  [Result] Logical meta size: {logical_meta_size}")

        new_log_meta_macro = cfg.format_size_macro(calc_meta_bytes)
        cfg.set_define(
            "LOGICAL_META_SIZE", new_log_meta_macro, "Auto-calculated from size.py"
        )

    # 9. OP Ratio
    meta_op = prompt_input("Meta OP ratio", default=0.07, convert_fn=float)

    # 10. Physical Meta
    n_chl = cfg.get_define_eval("NAND_CHANNELS")
    luns_per_chn = cfg.get_define_eval("LUNS_PER_NAND_CH")
    phy_blksize_bytes = block_size.to_bytes() * n_pln * luns_per_chn * n_chl

    pslc_times = {"TLC": 3, "QLC": 4}
    flash_factor = pslc_times.get(flash_type, 3)
    phys_meta = logical_meta_size.to_bytes()
    # Add OP and convert to pSLC/Flash consumption
    raw_phys_meta_with_op = phys_meta * (1 + meta_op) * flash_factor
    n_meta_sblks = math.ceil(raw_phys_meta_with_op / phy_blksize_bytes)
    if n_meta_sblks < 4:
        n_meta_sblks = 4
    phys_meta_with_op = int(n_meta_sblks * phy_blksize_bytes)
    new_phy_meta_macro = cfg.format_size_macro(phys_meta_with_op)
    cfg.set_define(
        "PHYSICAL_META_SIZE", new_phy_meta_macro, "Auto-calculated from size.py"
    )

    # --- Final Output Calculation (Visual only) ---
    # ... (Rest of your printing logic for physical sizes) ...

    print("\n----------------------------------")
    print(f"Summary:")
    print(f"  Prototype: {prototype}")
    print(f"  Flash: {flash_type} | Interface: {interface_type}")
    print(f"  Super Block: {SizedInt.from_bytes(sblk_size_bytes)}")
    print(
        f"  Meta Size: {logical_meta_size} Phy Meta Size: {SizedInt.from_bytes(phys_meta_with_op)}"
    )

    # Calculate total for insmod

    # Physical Data
    phys_data = logical_data_size.to_bytes()
    # Align to sblk
    if phys_data % sblk_size_bytes:
        phys_data = (phys_data // sblk_size_bytes + 1) * sblk_size_bytes
    # Add pSLC data area
    pslc_area = phy_blksize_bytes * npslc_sblks
    phys_data += pslc_area

    total_rsv = SizedInt(1, "M").to_bytes() + phys_data + phys_meta_with_op

    print(f"\nInsmod Command:")
    print(
        f"sudo insmod ./nvmev.ko memmap_start={memmap_start} memmap_size={SizedInt.from_bytes(total_rsv)} cpus=37,39"
    )
