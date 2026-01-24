import os
import sys

WORK_DIR = "/home/lab/mnt/home/lab/ydc/emulators/public/ConZone"

RMMOD_CMD = "sudo rmmod nvmev"
UMOUNT_CMD = "sudo umount mnt"


def process(rawdevice=False):
    if rawdevice == False:
        # umount
        print("\numount: %s" % (UMOUNT_CMD))
        ret = os.system(UMOUNT_CMD)

        if ret:
            print("Umount Error")
            return ret

    # rmmod
    print("\nrmmod: %s" % (RMMOD_CMD))
    os.system(RMMOD_CMD)
    os.system(f"sudo dmesg > {WORK_DIR}/log/rmmod_dmesg")
    return 0


# Usage: python3 umount.py
if __name__ == "__main__":
    os.chdir(WORK_DIR)
    if len(sys.argv) > 1 and sys.argv[1] == "rawdevice":
        ret = process(rawdevice=True)
    else:
        ret = process()
    if ret == 0:
        print("Umount Success")
