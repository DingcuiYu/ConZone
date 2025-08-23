import os
import sys
import contextlib

WORK_DIR = "/home/lab/mnt/home/lab/ydc/emulators/public/ConZone"

## TLC
# zoned sudo insmod ./nvmev.ko memmap_start=82G memmap_size=7657M cpus=7,8
# block sudo insmod ./nvmev.ko memmap_start=82G memmap_size=7465M cpus=7,8
## QLC
# zoned sudo insmod ./nvmev.ko memmap_start=82G memmap_size=8833M cpus=7,8
# block sudo insmod ./nvmev.ko memmap_start=82G memmap_size=8577M cpus=7,8
insmod_cmds = {
    "zoned": "insmod ./nvmev.ko memmap_start=82G memmap_size=7657M cpus=7,8",
    "block": "insmod ./nvmev.ko memmap_start=82G memmap_size=7465M cpus=7,8",
}

# sudo ./mkfs.f2fs -f -c /dev/nvme2n2 /dev/nvme2n1
# sudo ./mkfs.f2fs -f -m -c /dev/nvme2n2 /dev/nvme2n1
mkfs_cmds = {
    "zoned": "./mkfs.f2fs -f -m -c /dev/nvme2n2 /dev/nvme2n1",
    "block": "./mkfs.f2fs -f -c /dev/nvme2n2 /dev/nvme2n1",
}

MKFS_DIR = "f2fs-tools-1.14.0/build/sbin"


def prepare(interface, cell_mode):
    out_path = f"{WORK_DIR}/log/sqlite/{interface}"
    if os.path.exists(out_path):
        print("The output folder exists, now empty all files in it")
        os.system(f"rm -rf {out_path}")

    os.system(f"mkdir {out_path}")

    config_path = f"{WORK_DIR}/ssdconfigs"
    config_name = f"config-{cell_mode}-{interface}"
    os.system(f"mv ssd_config.h ssd_config.h.bak")
    os.system(f"cp {config_path}/{config_name} ./ssd_config.h")

    # compile
    ret = os.system("make -j `nproc`")
    if ret:
        print("Compile Error")
        return ret

    # copy configuration
    os.system(f"cp ssd_config.h {out_path}/")
    return 0


def process(interface, testfunc, *args):
    out_path = f"{WORK_DIR}/log/sqlite/{interface}/{args[0]}"
    if os.path.exists(out_path):
        print("The output folder exists, now empty all files in it")
        os.system(f"rm -rf {out_path}")
    os.system(f"mkdir {out_path}")

    # insmod
    os.system("dmesg -c > /dev/zero")

    print(f"\ninsmod: {insmod_cmds[interface]}")
    ret = os.system(insmod_cmds[interface])
    if ret:
        print("Insmod Error")
        return ret
    os.system(f"dmesg > {out_path}/insmod_dmesg")
    os.system("dmesg -c > /dev/zero")

    # mkfs.f2fs & mnt
    os.chdir(MKFS_DIR)
    print(f"\nmkfs: {mkfs_cmds[interface]}")
    ret = os.system(f"{mkfs_cmds[interface]} > {out_path}/mkfs_dmesg")
    if ret:
        print("mkfs.f2fs Error")
        return ret
    os.chdir(WORK_DIR)

    # MOUNT_CMD = "mount -o age_extent_cache,discard /dev/nvme2n1 mnt"
    print("\nmount: mount /dev/nvme2n1 mnt")
    ret = os.system("mount /dev/nvme2n1 mnt")
    if ret:
        print("mnt Error")
        return ret
    os.system(f"dmesg > {out_path}/mount_dmesg")
    os.system("dmesg -c > /dev/zero")

    # test
    ret = testfunc(out_path, *args)
    if ret:
        return ret

    # umount
    print("\numount: umount mnt")
    os.system("umount mnt")

    # rmmod
    os.system("dmesg -c > /dev/zero")
    print("\nrmmod: rmmod nvmev")
    os.system("rmmod nvmev")
    os.system(
        f"echo '---------------Benchmark: {args[0]} -----------------' >> {out_path}/ssd_statistic"
    )
    os.system(f"dmesg >> {out_path}/ssd_statistic")
    return 0


def sqlitefunc(out_path, *args):
    kwargs = args[1]

    mobibench_cmd = f'{WORK_DIR}/Mobibench/shell/mobibench -p ./mnt/mobibench -d {kwargs["db_mode"]} -n 2000000 -j {kwargs["journal_mode"]} -s 2 -D 1 -T 1 -q'

    print(f"mobibench: {mobibench_cmd}")
    os.system("dmesg -c > /dev/zero")
    # ret = os.system("cd %s && (%s >> %s/%s_mobibench)" % (mobibench_dir,mobibench_cmd,out_path,args[0]))
    # ret = os.system("(%s >> %s/%s_mobibench)" % (mobibench_cmd,out_path,args[0]))

    # os.system("strace -fp $(pidof mobibench) -T -e trace=fdatasync -o {%s/%s_strace}" % (out_path,args[0]))
    # strace_cmd = "strace -f -T -e trace=fdatasync -o {}/{}_strace".format(out_path, args[0])
    strace_cmd = ""
    mobibench_cmd_with_strace = f"{strace_cmd} {mobibench_cmd}"

    ret = os.system(f"{mobibench_cmd_with_strace} >> {out_path}/{args[0]}_mobibench")

    os.system(
        f'echo "\n--------------------NEW TEST---------------------\n" >> {out_path}/{args[0]}_wlog'
    )

    os.system(f"dmesg >> {out_path}/{args[0]}_wlog")
    os.system(
        f'echo "\n--------------------db file size---------------------\n" >> {out_path}/{args[0]}_wlog'
    )

    os.system(f"stat {WORK_DIR}/mnt/mobibench/test.db0_0 >>  {out_path}/{args[0]}_wlog")

    if ret:
        print("Error in sqlitefunc")
        return ret
    return 0


def exp(interface):
    benchmarks = {
        "WAL-insert": (sqlitefunc, {"db_mode": 0, "journal_mode": 3}),
        "WAL-update": (sqlitefunc, {"db_mode": 1, "journal_mode": 3}),
        "RBJ_delete-insert": (sqlitefunc, {"db_mode": 0, "journal_mode": 0}),
        "RBJ_delete-update": (sqlitefunc, {"db_mode": 1, "journal_mode": 0}),
        "RBJ_truncate-insert": (sqlitefunc, {"db_mode": 0, "journal_mode": 1}),
        "RBJ_truncate-update": (sqlitefunc, {"db_mode": 1, "journal_mode": 1}),
        "RBJ_persistent-insert": (sqlitefunc, {"db_mode": 0, "journal_mode": 2}),
        "RBJ_persistent-update": (sqlitefunc, {"db_mode": 1, "journal_mode": 2}),
    }

    for bm in benchmarks:
        kargs = benchmarks[bm][1]
        ret = process(interface, benchmarks[bm][0], bm, kargs)
        if ret:
            return ret


# Usage: python3 sqlite.py
if __name__ == "__main__":
    out_path = f"{WORK_DIR}/log/sqlite"
    os.chdir(WORK_DIR)
    if os.path.exists(out_path):
        print("The output folder does not exists, now empty all files in it")
        os.system(f"rm -rf {out_path}")

    os.system(f"mkdir {out_path}")
    for interface in {"zoned", "block"}:
        ret = prepare(interface, "tlc")
        if ret == 0:
            r = exp(interface)
