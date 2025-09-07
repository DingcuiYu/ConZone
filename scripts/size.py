import math
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
    def from_str(cls, s: str):
        """
        Construct a SizedInt object from a string like '4M' or '2.5G'.
        """
        s = s.strip().upper()
        match = re.fullmatch(r"([\d.]+)([BKMGTP])", s)
        if not match:
            raise ValueError(f"Invalid format: {s}. Expected format like '4K', '2.5G'")
        val, unit = match.groups()
        return cls(float(val), unit)

    def __repr__(self):
        return self._format()

    def _format(self):
        """Formatting of values into auto-standardized units"""
        val = self.value_in_bytes
        ui = 0
        while val >= self.BASE and ui < len(self.SCALE_UNIT) - 1:
            tmp = val / self.BASE
            if not tmp.is_integer():
                break
            val = tmp
            ui += 1
        return f"{int(val)}{self.SCALE_UNIT[ui]}"

    def _check_type(self, other):
        if not isinstance(other, SizedInt):
            raise TypeError("Operand must be SizedInt")

    def __add__(self, other):
        self._check_type(other)
        return SizedInt(self.value_in_bytes + other.value_in_bytes, "B")

    def __sub__(self, other):
        self._check_type(other)
        result = self.value_in_bytes - other.value_in_bytes
        if result < 0:
            raise ValueError("Result would be negative")
        return SizedInt(result, "B")

    def __mul__(self, other):
        if isinstance(other, (int, float)):
            return SizedInt.from_bytes(self.value_in_bytes * other)
        return NotImplemented

    def __floordiv__(self, other):
        if isinstance(other, (int, float)):
            return SizedInt.from_bytes(self.value_in_bytes // other)
        elif isinstance(other, SizedInt):
            return int(self.value_in_bytes // other.value_in_bytes)
        return NotImplemented

    def __mod__(self, other):
        self._check_type(other)
        return int(self.value_in_bytes % other.value_in_bytes)

    def __eq__(self, other):
        return self.value_in_bytes == other.value_in_bytes

    def __lt__(self, other):
        return self.value_in_bytes < other.value_in_bytes

    def __le__(self, other):
        return self.value_in_bytes <= other.value_in_bytes

    def __gt__(self, other):
        return self.value_in_bytes > other.value_in_bytes

    def __ge__(self, other):
        return self.value_in_bytes >= other.value_in_bytes

    def to(self, unit):
        """Convert to specified units"""
        if unit not in self.UNIT_SCALE:
            raise ValueError(f"Invalid unit: {unit}")
        val = self.value_in_bytes / (self.BASE ** self.UNIT_SCALE[unit])
        return val

    def to_tuple(self):
        """Returns a representation of (value, unit) (normalized)"""
        val = self.value_in_bytes
        ui = 0
        while val >= self.BASE and ui < len(self.SCALE_UNIT) - 1:
            val /= self.BASE
            ui += 1
        return (val, self.SCALE_UNIT[ui])

    def to_bytes(self):
        """Return byte representation (float)"""
        return self.value_in_bytes


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


def lcm_sizeint(a: SizedInt, b: SizedInt) -> SizedInt:
    """
    Compute the least common multiple (LCM) of two SizedInt objects.

    Args:
        a (SizedInt): First size.
        b (SizedInt): Second size.

    Returns:
        SizedInt: The LCM, as a new SizedInt object (in bytes).
    """
    bytes_a = int(a.to_bytes())
    bytes_b = int(b.to_bytes())
    lcm_bytes = (bytes_a * bytes_b) // math.gcd(bytes_a, bytes_b)
    return SizedInt.from_bytes(lcm_bytes)


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


if __name__ == "__main__":
    oneshotpg_size = {"TLC": SizedInt(96, "K"), "QLC": SizedInt(128, "K")}
    pslc_times = {"TLC": 3, "QLC": 4}

    default_blocksize = {"TLC": "24M", "QLC": "32M"}
    default_memmapstart = "82G"
    # data namespace = 4GiB
    default_metasize = {"zoned": "256M", "block": "40M"}
    default_pslcblks = {"conzone": 28, "zns": 0}

    prototype = prompt_input(
        "Please enter the prototype of the emulator (conzone or zns): ",
        default="conzone",
        convert_fn=str,
        type_name="string",
    )

    memmap_start = prompt_input(
        "Please enter the start address of memmap (e.g., 82G): ",
        default=default_memmapstart,
        convert_fn=str,
        type_name="string",
    )

    flash_type = prompt_input(
        "Please enter the flash type (e.g., TLC, QLC): ",
        default="TLC",
        convert_fn=str,
        type_name="string",
    )
    if flash_type not in default_blocksize:
        print("Undefined flash type. Set flash_type == TLC for test\n")
        flash_type = "TLC"

    interface_type = prompt_input(
        "Please enter the interface type (e.g., block, zoned): ",
        default="zoned",
        convert_fn=str,
        type_name="string",
    )
    if interface_type not in default_metasize:
        print("Undefined interface type. Set interface_type == zoned for test\n")
        interface_type = "zoned"

    block_size = prompt_input(
        "Please enter blcok size (e.g., 2M, 32M): ",
        default=default_blocksize[flash_type],
        convert_fn=SizedInt.from_str,
        type_name="size",
    )

    dies = prompt_input(
        "Please enter the number of dies per superblock:", default=4, convert_fn=int
    )

    npslc_sblks = prompt_input(
        "Please enter the number of pSLC superblocks for data area:",
        default=default_pslcblks[prototype],
        convert_fn=int,
    )

    logical_data_size = prompt_input(
        "Please enter the size of data namespace (e.g., 4G): ",
        default="4G",
        convert_fn=SizedInt.from_str,
        type_name="size",
    )

    logical_meta_size = prompt_input(
        "Please enter the size of meta namespace (e.g., 256M, 40M): ",
        default=default_metasize[interface_type],
        convert_fn=SizedInt.from_str,
        type_name="size",
    )

    meta_op = prompt_input(
        "Enter the OP ratio for meta area",
        default=0.07,
        convert_fn=float,
        type_name="float",
    )

    data_op = 0
    if interface_type == "block":
        data_op = meta_op
        print(f"the OP ratio for data area is {data_op}")

    print("----------------------------------")

    if block_size % oneshotpg_size[flash_type]:
        block_size = oneshotpg_size[flash_type] * ceildiv(
            block_size, oneshotpg_size[flash_type]
        )
        print(f"[WARN] block_size should be {block_size}")

    physical_data_size = logical_data_size
    if physical_data_size % block_size:
        physical_data_size = block_size * ceildiv(physical_data_size, block_size)
        print(f"[WARN] physical_data_size should be {physical_data_size}")

    sblk_size = block_size * dies
    ndata_sblks = ceildiv(physical_data_size, sblk_size)
    aligned_blk_size = SizedInt(2 ** math.ceil(math.log2(block_size.to("K"))), "K")

    if interface_type == "zoned":
        zone_size = aligned_blk_size * dies
        if logical_data_size % zone_size:
            logical_data_size = zone_size * ndata_sblks
            print(f"[WARN] logical_data_size should be {logical_data_size}")
    else:
        zone_size = SizedInt(2, "M")

    print(f"[Zone Size] {zone_size}")

    if prototype == "conzone" and npslc_sblks < 4:
        npslc_sblks = 4
        print(f"[WARN] npslc_sblks should be aleast {npslc_sblks}")

    print(f"[Super Block Size] {sblk_size}")

    pslc_data_size = sblk_size * npslc_sblks
    print(
        f"[pSLC Flash Block Size] {pslc_data_size} [pSLC Flash Block Capacity] {sblk_size * npslc_sblks // pslc_times[flash_type]}"
    )
    physical_data_size_withop = logical_data_size * (1 + data_op)

    while physical_data_size < physical_data_size_withop:
        physical_data_size = physical_data_size + sblk_size

    physical_data_size = physical_data_size + pslc_data_size
    print(f"[Physical Data Size] {physical_data_size}")

    if logical_meta_size % zone_size:
        logical_meta_size = zone_size * ceildiv(logical_meta_size, zone_size)
        print(f"[WARN] logical_meta_size should be {logical_meta_size}")

    physical_meta_size_withop = logical_meta_size * (1 + meta_op)
    physical_meta_size_withop = physical_meta_size_withop * pslc_times[flash_type]
    nmeta_sblks = ceildiv(logical_meta_size, sblk_size)
    physical_meta_size = sblk_size * nmeta_sblks

    while physical_meta_size < physical_meta_size_withop or nmeta_sblks < 4:
        physical_meta_size = physical_meta_size + sblk_size
        nmeta_sblks = ceildiv(physical_meta_size, sblk_size)

    print(f"[Physical Meta Size] {physical_meta_size}")

    nvmev_rsv_size = SizedInt(1, "M")

    rsv_size = nvmev_rsv_size + physical_data_size + physical_meta_size
    print("Insmod Command:")
    print(
        f"sudo insmod ./nvmev.ko memmap_start={memmap_start} memmap_size={rsv_size} cpus=7,8"
    )
