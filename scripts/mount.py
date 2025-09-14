import os

WORK_DIR = "/home/lab/mnt/home/lab/ydc/emulators/public/ConZone"

META_DEVICE = "/dev/nvme2n1"
DATA_DEVICE = "/dev/nvme2n2"

# make -j `nproc`

## 4 GiB
### TLC
#### zoned sudo insmod ./nvmev.ko memmap_start=82G memmap_size=7657M cpus=7,8
#### block sudo insmod ./nvmev.ko memmap_start=82G memmap_size=7465M cpus=7,8
### QLC
#### block sudo insmod ./nvmev.ko memmap_start=82G memmap_size=8577M cpus=7,8
#### zoned sudo insmod ./nvmev.ko memmap_start=82G memmap_size=8833M cpus=7,8

## 64 GiB
### TLC
#### block insmod ./nvmev.ko memmap_start=82G memmap_size=74089M cpus=7,8
#### zoned insmod ./nvmev.ko memmap_start=82G memmap_size=69961M cpus=7,8

### QLC
#### block insmod ./nvmev.ko memmap_start=82G memmap_size=75393M cpus=7,8
#### zoned insmod ./nvmev.ko memmap_start=82G memmap_size=71425M cpus=7,8
INSMOD_CMD = f"sudo insmod ./nvmev.ko memmap_start=82G memmap_size=75393M cpus=7,8"

# MKFS_DIR = "f2fs-tools-1.14.0/build/sbin"
MKFS_DIR = "/home/lab/mnt/home/lab/ydc/f2fs-tools-1.15.0/mkfs"
MKFS_CMD = f"sudo ./mkfs.f2fs -f -m {DATA_DEVICE}"
# MKFS_CMD = f"sudo ./mkfs.f2fs -f -c {DATA_DEVICE} {META_DEVICE}"
# block sudo ./mkfs.f2fs -f -c /dev/nvme2n2 /dev/nvme2n1
# zone sudo ./mkfs.f2fs -f -m -c /dev/nvme2n2 /dev/nvme2n1

# MOUNT_CMD = "mount -o age_extent_cache,discard /dev/nvme2n1 mnt"
MOUNT_CMD = f"sudo mount {DATA_DEVICE} mnt"


def prepare():
    # compile
    ret = os.system("make -j `nproc`")
    if ret:
        print("Compile Error")
        return ret
    return 0


def process():
    # insmod
    print(f"\ninsmod: {INSMOD_CMD}")
    ret = os.system(INSMOD_CMD)
    if ret:
        print("Insmod Error")
        return ret
    os.system(f"sudo dmesg > {WORK_DIR}/log/insmod_dmesg")
    os.system("sudo dmesg -c > /dev/zero")

    # mkfs.f2fs & mnt
    os.chdir(MKFS_DIR)
    print(f"\nmkfs: {MKFS_CMD}")
    ret = os.system(f"{MKFS_CMD} > {WORK_DIR}/log/mkfs_dmesg")
    if ret:
        print("mkfs.f2fs Error")
        return ret
    os.chdir(WORK_DIR)

    print(f"\nmount: {MOUNT_CMD}")
    ret = os.system(MOUNT_CMD)
    if ret:
        print("mnt Error")
        return ret
    os.system(f"sudo dmesg > {WORK_DIR}/log/mount_dmesg")
    os.system("sudo dmesg -c > /dev/zero")
    return 0


# Usage: python3 mount.py
if __name__ == "__main__":
    os.chdir(WORK_DIR)
    ret = prepare()
    if ret == 0:
        process()
