import os
import sys
import contextlib

WORK_DIR = "/home/lab/mnt/home/lab/ydc/emulators/public/ConZone"
OUT_DIR = f"log/{sys.argv[2]}"

META_DEVICE = "/dev/nvme2n1"
DATA_DEVICE = "/dev/nvme2n2"

zoned_memmapsize = 9729
block_memmapsize = 74989
memmapsize = block_memmapsize
if sys.argv[1] == "zoned":
    memmapsize = zoned_memmapsize

zoned_options = "-f -m -c"
block_options = "-f -c"
options = block_options
if sys.argv[1] == "zoned":
    options = zoned_options

# make -j `nproc`
# sudo insmod ./nvmev.ko memmap_start=82G memmap_size=9729M cpus=7,8 (zonedslc 7 pslc)
# insmod ./nvmev.ko memmap_start=82G memmap_size=74989M cpus=7,8
# insmod ./nvmev.ko memmap_start=82G memmap_size=70529M cpus=7,8
INSMOD_CMD = f"insmod ./nvmev.ko memmap_start=82G memmap_size={memmapsize}M cpus=7,8"
RMMOD_CMD = "rmmod nvmev"

MKFS_DIR = "f2fs-tools-1.14.0/build/sbin"
MKFS_CMD = f"./mkfs.f2fs {options} {DATA_DEVICE} {META_DEVICE}"
# sudo ./mkfs.f2fs -f -c /dev/nvme2n2 /dev/nvme2n1
# sudo ./mkfs.f2fs -f -m -c /dev/nvme2n2 /dev/nvme2n1

# MOUNT_CMD = "mount -o age_extent_cache,discard /dev/nvme2n1 mnt"
MOUNT_CMD = f"mount {META_DEVICE} mnt"
UMOUNT_CMD = "umount mnt"


def prepare():
    if os.path.exists(OUT_DIR):
        print("The output folder exists, now empty all files in it")
        os.system(f"rm -rf {OUT_DIR}")

    os.system(f"mkdir {OUT_DIR}")

    # compile
    ret = os.system("make -j `nproc`")
    if ret:
        print("Compile Error")
        return ret

    # copy configuration
    os.system(f"cp ssd_config.h {OUT_DIR}/")
    return 0


def process(testfunc, *args):
    # insmod
    os.system("dmesg -c > /dev/zero")

    print(f"\ninsmod: {INSMOD_CMD}")
    ret = os.system(INSMOD_CMD)
    if ret:
        print("Insmod Error")
        return ret
    os.system(f"dmesg > {OUT_DIR}/config")
    os.system("dmesg -c > /dev/zero")

    # mkfs.f2fs & mnt
    os.chdir(MKFS_DIR)
    print(f"\nmkfs: {MKFS_CMD}")
    ret = os.system(f"{MKFS_CMD} > {WORK_DIR}/{OUT_DIR}/mkfs_info")
    if ret:
        print("mkfs.f2fs Error")
        return ret
    os.chdir(WORK_DIR)

    print(f"\nmount: {MOUNT_CMD}")
    ret = os.system(MOUNT_CMD)
    if ret:
        print("mnt Error")
        return ret
    os.system(f"dmesg >> {OUT_DIR}/{args[0]}_mount_wlog")
    os.system("dmesg -c > /dev/zero")

    # test
    ret = testfunc(*args)
    if ret:
        return ret

    # umount
    print("\numount: %s" % (UMOUNT_CMD))
    os.system(UMOUNT_CMD)

    # rmmod
    os.system("dmesg -c > /dev/zero")
    print("\nrmmod: %s" % (RMMOD_CMD))
    os.system(RMMOD_CMD)
    os.system(
        "echo '---------------Benchmark: %s -----------------' >> %s/ssd_statistic"
        % (args[0], OUT_DIR)
    )
    os.system("dmesg >> %s/ssd_statistic" % (OUT_DIR))
    return 0


def fiofunc(*args):
    kwargs = args[1]
    # for read cmd
    if kwargs["rw"] == "read" or kwargs["rw"] == "randread":
        w_cmd = "fio --name=1 --filename=mnt/test --numjobs=1 --buffered=0 --rw=write --bs=128k --size=4g"
        print("fio pre write: %s" % (w_cmd))
        ret = os.system("cd %s && %s" % (WORK_DIR, w_cmd))
        if ret:
            return ret

    if kwargs["conf"] == 0:
        if kwargs["rw"] == "randread":
            fio_args = "--name=1 --filename=mnt/test --numjobs={jobs} --buffered={buffered} --rw={rw} --bs={bs} --size={size} --io_size={io_size} --fsync={fsync} --norandommap={norandommap} --group_reporting".format(
                jobs=kwargs["jobs"],
                buffered=kwargs["buffered"],
                rw=kwargs["rw"],
                bs=kwargs["bs"],
                size=kwargs["size"],
                io_size=kwargs["io_size"],
                fsync=kwargs["fsync"],
                norandommap=kwargs["norandommap"],
            )
        else:
            fio_args = "--name=1 --filename=mnt/test --numjobs={jobs} --buffered={buffered} --rw={rw} --bs={bs} --size={size} --io_size={io_size} --fsync={fsync} --random_distribution={random_distribution} --group_reporting".format(
                jobs=kwargs["jobs"],
                buffered=kwargs["buffered"],
                rw=kwargs["rw"],
                bs=kwargs["bs"],
                size=kwargs["size"],
                io_size=kwargs["io_size"],
                fsync=kwargs["fsync"],
                random_distribution=kwargs["random_distribution"],
            )
    else:
        fio_args = "{workdir}/fioconfs/{conffile}".format(
            workdir=WORK_DIR, conffile=kwargs["conffile"]
        )

    fio_cmd = "fio {args}".format(args=fio_args)

    print("fio: %s" % (fio_cmd))
    os.system("dmesg -c > /dev/zero")
    ret = os.system(
        "cd %s && (%s >> %s/%s_fio)" % (WORK_DIR, fio_cmd, OUT_DIR, args[0])
    )
    os.system(
        'echo "\n--------------------NEW TEST---------------------\n" >> %s/%s_wlog'
        % (OUT_DIR, args[0])
    )
    os.system("dmesg >> %s/%s_wlog" % (OUT_DIR, args[0]))
    if ret:
        print("Error in fiofunc")
        return ret
    return 0


def fiofunc_aged(*args):
    kwargs = args[1]
    # create file
    # sudo fio --name=1 --filename=mnt/test --numjobs=1 --buffered=0 --rw=write --bs=128k --size=1g
    w_cmd = "fio --name=1 --filename=mnt/test --numjobs=1 --buffered=0 --rw=write --bs=512k --size={size}".format(
        size=kwargs["size"]
    )
    print("fio pre write: %s" % (w_cmd))
    # ret = os.system("cd %s && (%s >> %s/%s_pre-fio)" % (WORK_DIR,w_cmd,OUT_DIR,args[0]))
    os.system("cd %s" % (WORK_DIR))
    ret = os.system("%s" % (w_cmd))
    os.system("dmesg >> %s/%s_prewrite_log" % (OUT_DIR, args[0]))
    if ret:
        print("Error in prewrite")
        return ret

    if sys.argv[1] == "zoned":
        ret = os.system(
            "blkzone report {data_device} > /dev/zero".format(data_device=DATA_DEVICE)
        )
        if ret:
            print("Error in blkzone report")
            return ret
        os.system("dmesg >> %s/%s_prewrite_log" % (OUT_DIR, args[0]))

    # overwrite
    # fio --name=1 --filename=mnt/test --numjobs=1 --buffered=0 --rw=write --bs=4k --size=3960m --io_size=16g --overwrite=1 --norandommap
    if kwargs["conf"] == 0:
        fio_args = "--name=1 --filename=mnt/test --numjobs=1 --buffered=0 --rw=randwrite --bs=4k --size={size} --io_size=16g  --overwrite=1 --norandommap".format(
            size=kwargs["size"]
        )
    else:
        fio_args = "{workdir}/fioconfs/{conffile}".format(
            workdir=WORK_DIR, conffile=kwargs["conffile"]
        )

    fio_cmd = "fio {args}".format(args=fio_args)

    print("fio: %s" % (fio_cmd))
    os.system("dmesg -c > /dev/zero")
    # ret = os.system("cd %s && (%s >> %s/%s_fio)" % (WORK_DIR,fio_cmd,OUT_DIR,args[0]))
    os.chdir(WORK_DIR)
    ret = os.system(f"{fio_cmd} 2>&1 | tee {OUT_DIR}/{args[0]}_fio")
    os.system(
        'echo "\n--------------------NEW TEST---------------------\n" >> %s/%s_wlog'
        % (OUT_DIR, args[0])
    )
    os.system("dmesg >> %s/%s_wlog" % (OUT_DIR, args[0]))
    if ret:
        print("Error in fiofunc")
        return ret
    return 0


def sqlitefunc(*args):
    kwargs = args[1]

    mobibench_dir = "%s/Mobibench/shell/" % (WORK_DIR)
    mobibench_cmd = "{dir}mobibench -p ./mnt/mobibench -d {db_mode} -n 1000000 -j {journal_mode} -s 2 -D 1 -T 1 -q".format(
        dir=mobibench_dir,
        db_mode=kwargs["db_mode"],
        journal_mode=kwargs["journal_mode"],
    )

    print("mobibench: %s" % (mobibench_cmd))
    os.system("dmesg -c > /dev/zero")
    # ret = os.system("cd %s && (%s >> %s/%s_mobibench)" % (mobibench_dir,mobibench_cmd,OUT_DIR,args[0]))
    # ret = os.system("(%s >> %s/%s_mobibench)" % (mobibench_cmd,OUT_DIR,args[0]))
    # os.system("strace -fp $(pidof mobibench) -T -e trace=fdatasync -o {%s/%s_strace}" % (OUT_DIR,args[0]))
    # strace_cmd = "strace -f -T -e trace=fdatasync -o {}/{}_strace".format(OUT_DIR, args[0])
    strace_cmd = ""
    mobibench_cmd_with_strace = "{} {}".format(strace_cmd, mobibench_cmd)

    ret = os.system(
        "({} >> {}/{}_mobibench)".format(mobibench_cmd_with_strace, OUT_DIR, args[0])
    )

    os.system(
        'echo "\n--------------------NEW TEST---------------------\n" >> %s/%s_wlog'
        % (OUT_DIR, args[0])
    )
    os.system("dmesg >> %s/%s_wlog" % (OUT_DIR, args[0]))
    os.system(
        'echo "\n--------------------db file size---------------------\n" >> %s/%s_wlog'
        % (OUT_DIR, args[0])
    )
    os.system(
        "stat %s/mnt/mobibench/test.db0_0 >>  %s/%s_wlog" % (WORK_DIR, OUT_DIR, args[0])
    )
    if ret:
        print("Error in sqlitefunc")
        return ret
    return 0


def exp():
    block_size = ("4k", "8k", "16k", "32k", "64k", "128k", "256k", "512k")
    benchmarks = {
        "fio-aged-60": (fiofunc_aged, {"conf": 0, "size": "2458m"}),
        "fio-aged-75": (fiofunc_aged, {"conf": 0, "size": "3455m"}),
        # "fio-mot-gc":(fiofunc,{"conf":1,"conffile":"mot-gc.conf","rw":"randwrite"}),
        # "fio-rw-MT":(fiofunc,{"conf":0,"jobs":4,"buffered":0,"rw":"randrw","bs":"4k","size":"7g",  "io_size":"7g","fsync":0,"random_distribution":"random"}),
        #     "fio-zoned-ST":(fiofunc,{"conf":0,"jobs":1,"buffered":0,"rw":"randwrite","bs":"4k","size":"16g","io_size":"16g","fsync":0,"random_distribution":"zoned:60/10:30/20:8/30:2/40"}),
        #     "fio-zipf-ST":(fiofunc,{"conf":0,"jobs":1,"buffered":0,"rw":"randwrite","bs":"4k","size":"16g","io_size":"16g","fsync":0,"random_distribution":"zipf:1.2"}),
        #     "fio-normal-ST":(fiofunc,{"conf":0,"jobs":1,"buffered":0,"rw":"randwrite","bs":"4k","size":"16g","io_size":"16g","fsync":0,"random_distribution":"normal"}),
        #     "fio-random-ST":(fiofunc,{"conf":0,"jobs":1,"buffered":0,"rw":"randwrite","bs":"4k","size":"16g","io_size":"16g","fsync":0,"random_distribution":"random"}),
        #    "fio-seqw-ST":(fiofunc,{"conf":0,"jobs":1,"buffered":0,"rw":"write","bs":"512k","size":"16g","io_size":"16g","fsync":0,"random_distribution":"random"}),
        #    "fio-seqw-MT":(fiofunc,{"conf":1,"conffile":"SeqW-MT.conf","rw":"write"}),
        # #    "fio-randw-ST":(fiofunc,{"conf":0,"jobs":1,"buffered":0,"rw":"randwrite","bs":"4k","size":"16g","io_size":"16g","fsync":0,"random_distribution":"random"}),
        #    "fio-randw-MT":(fiofunc,{"conf":1,"conffile":"RandW-MT.conf","rw":"randwrite"}),
        #    "fio-fsync-ST":(fiofunc,{"conf":0,"jobs":1,"buffered":1,"rw":"randwrite","bs":"4k","size":"16g","io_size":"16g","fsync":1,"random_distribution":"random"}),
        #    "fio-fsync-MT":(fiofunc,{"conf":1,"conffile":"BufW-MT.conf","rw":"randwrite"}),
        #    "fio-seqr-ST":(fiofunc,{"conf":0,"jobs":1,"buffered":0,"rw":"read","bs":"512k","size":"4g","io_size":"16g","fsync":0,"random_distribution":"random"}),
        #    "fio-seqr-MT":(fiofunc,{"conf":0,"jobs":4,"buffered":0,"rw":"read","bs":"512k","size":"1g","io_size":"4g","fsync":0,"random_distribution":"random"}),
        #    "fio-randr-ST":(fiofunc,{"conf":0,"jobs":1,"buffered":0,"rw":"randread","bs":"4k","size":"4g","io_size":"16g","fsync":0,"norandommap":1}),
        #    "fio-randr-MT":(fiofunc,{"conf":0,"jobs":4,"buffered":0,"rw":"randread","bs":"4k","size":"1g",  "io_size":"4g","fsync":0,"norandommap":1}),
        # "WAL-insert":(sqlitefunc,{'db_mode':0,"journal_mode":3}),
        # "WAL-update":(sqlitefunc,{'db_mode':1,"journal_mode":3}),
        # "RBJ_delete-insert":(sqlitefunc,{'db_mode':0,"journal_mode":0}),
        # "RBJ_delete-update":(sqlitefunc,{'db_mode':1,"journal_mode":0}),
        # "RBJ_truncate-insert":(sqlitefunc,{'db_mode':0,"journal_mode":1}),
        # "RBJ_truncate-update":(sqlitefunc,{'db_mode':1,"journal_mode":1}),
        # "RBJ_persistent-insert":(sqlitefunc,{'db_mode':0,"journal_mode":2}),
        # "RBJ_persistent-update":(sqlitefunc,{'db_mode':1,"journal_mode":2}),
    }

    for bm in benchmarks:
        kargs = benchmarks[bm][1]
        if bm == "fio-seqw-ST" or bm == "fio-seqr-ST" or bm == "fio-seqr-MT":
            for bs in block_size:
                kargs["bs"] = bs
                ret = process(benchmarks[bm][0], bm, kargs)
                if ret:
                    return ret
        elif bm == "fio-seqw-MT":
            for bs in block_size:
                kargs["conffile"] = "seqw-mt/{}.conf".format(bs)
                ret = process(benchmarks[bm][0], bm, kargs)
                if ret:
                    return ret
        else:
            if bm == "fio-aged-60":
                if sys.argv[1] == "zoned":
                    kargs["size"] = "2073m"
                else:
                    kargs["size"] = "2376m"
            if bm == "fio-aged-75":
                if sys.argv[1] == "zoned":
                    kargs["size"] = "2591m"
                else:
                    kargs["size"] = "2970m"
            if bm == "fio-aged-90":
                if sys.argv[1] == "zoned":
                    kargs["size"] = "3110m"
                else:
                    kargs["size"] = "3564m"
            if bm == "fio-aged-100":
                if sys.argv[1] == "zoned":
                    kargs["size"] = "3455m"
                else:
                    kargs["size"] = "3960m"

            ret = process(benchmarks[bm][0], bm, kargs)
            if ret:
                return ret
        # break


# Usage: python3 exp.py [block/zoned] [scheme_name]
if __name__ == "__main__":
    os.chdir(WORK_DIR)
    ret = prepare()
    if ret == 0:
        r = exp()
