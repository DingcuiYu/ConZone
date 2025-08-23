import os

WORK_DIR = "/home/lab/mnt/home/lab/ydc/emulators/public/ConZone"

RMMOD_CMD = "sudo rmmod nvmev"
UMOUNT_CMD = "sudo umount mnt"


def process():
    # umount
    print("\numount: %s" % (UMOUNT_CMD))
    os.system(UMOUNT_CMD)

    # rmmod
    print("\nrmmod: %s" % (RMMOD_CMD))
    os.system(RMMOD_CMD)
    os.system(f"sudo dmesg > {WORK_DIR}/log/rmmod_dmesg")
    return 0


# Usage: python3 exp.py [block/zoned] [scheme_name]
if __name__ == "__main__":
    os.chdir(WORK_DIR)
    ret = process()
