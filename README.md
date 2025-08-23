# ConZone

## Introduction

ConZone is a versatile software-defined consumer-level virtual zoned device. It is developed based on [NVMeVirt](https://github.com/snu-csl/nvmevirt), which is implemented as a Linux kernel module.

Further details on the design and implementation of ConZone can be found in the following papers.

[(DATE 2025) ConZone: A Zoned Flash Storage Emulator for Consumer Devices](https://ieeexplore.ieee.org/document/10993146?denied=)


Please feel free to contact us at [dingcuiy@gamil.com] if you have any questions or suggestions.

## ConZone QuickStart

[Chinese Version](https://invincible-ursinia-dbd.notion.site/QuickStart-ConZone-2588d13c675080c7ba5ddde5255e5612?source=copy_link)

### Kernel Version and Environment

* **Requirement:** v6.10 and above (Recommended: v6.12.16)
* **My Environment:** Ubuntu 22.04.5 LTS (6.12.16)
* **Note:** If you use a lower kernel version, the Kernel Code/Data might occupy the reserved space. For the reason, please see https://github.com/snu-csl/nvmevirt/issues/60#issuecomment-2687864955.

---
### Modifying Grub Boot Options
Edit the `GRUB_CMDLINE_LINUX` in `/etc/default/grub`. Separate multiple options with a space.

#### 1. Reserve Physical Memory
NVMeVirt is a DRAM-based emulator and requires a certain amount of physical memory to be reserved. The reserved size is the size of the storage device.

For example, if the server is equiped with about 160GiB of physical memory and I want to emulate a 64GiB flash storage, I can use:

`GRUB_CMDLINE_LINUX="memmap=79G\\\$82G"`

Here, a total of 79GiB of memory is reserved because of the user-invisible SLC area.

**P.S.** After updating the grub and rebooting, you can verify if the reservation was successful by checking the current contiguous memory layout with `sudo cat /proc/iomem`.

#### 2. [Optional] Reserve Dedicated Cores for NVMeVirt's Dispatcher and I/O Thread
To prevent NVMeVirt from interfering with applications, you can reserve dedicated cores for the dispatcher and I/O threads:

`GRUB_CMDLINE_LINUX="memmap=79G\$82G isolcpus=7,8"`

#### 3. Update Grub and Reboot the System
```bash
sudo update-grub
sudo reboot
```
---
### Download Source Code
```bash
git clone git@github.com:DingcuiYu/ConZone.git
```

### Compile
```bash
cd ConZone
make -j `nproc`
```
A successful compilation will look like this: (P.S. `zonelinux-6.12.16` is my custom kernel)

```
make -C /lib/modules/6.12.16-custom+/build M=/home/lab/mnt/home/lab/ydc/emulators/public/ConZone modules
make[1]: Entering directory '/home/lab/mnt/home/lab/ydc/linux/zonelinux-6.12.16'
warning: the compiler differs from the one used to build the kernel
  The kernel was built by: gcc (Ubuntu 12.3.0-1ubuntu1~22.04) 12.3.0
  You are using:           gcc (Ubuntu 12.3.0-1ubuntu1~22.04.2) 12.3.0
  CC [M]  /home/lab/mnt/home/lab/ydc/emulators/public/ConZone/main.o
  CC [M]  /home/lab/mnt/home/lab/ydc/emulators/public/ConZone/pci.o
  CC [M]  /home/lab/mnt/home/lab/ydc/emulators/public/ConZone/admin.o
  CC [M]  /home/lab/mnt/home/lab/ydc/emulators/public/ConZone/io.o
  CC [M]  /home/lab/mnt/home/lab/ydc/emulators/public/ConZone/dma.o
  CC [M]  /home/lab/mnt/home/lab/ydc/emulators/public/ConZone/ssd.o
  CC [M]  /home/lab/mnt/home/lab/ydc/emulators/public/ConZone/zns_ftl.o
  CC [M]  /home/lab/mnt/home/lab/ydc/emulators/public/ConZone/zns_read_write.o
  CC [M]  /home/lab/mnt/home/lab/ydc/emulators/public/ConZone/zms_read_write.o
  CC [M]  /home/lab/mnt/home/lab/ydc/emulators/public/ConZone/zns_mgmt_send.o
  CC [M]  /home/lab/mnt/home/lab/ydc/emulators/public/ConZone/zns_mgmt_recv.o
  CC [M]  /home/lab/mnt/home/lab/ydc/emulators/public/ConZone/channel_model.o
  CC [M]  /home/lab/mnt/home/lab/ydc/emulators/public/ConZone/conv_ftl.o
  CC [M]  /home/lab/mnt/home/lab/ydc/emulators/public/ConZone/simple_ftl.o
  CC [M]  /home/lab/mnt/home/lab/ydc/emulators/public/ConZone/pqueue/pqueue.o
  LD [M]  /home/lab/mnt/home/lab/ydc/emulators/public/ConZone/nvmev.o
  MODPOST /home/lab/mnt/home/lab/ydc/emulators/public/ConZone/Module.symvers
  CC [M]  /home/lab/mnt/home/lab/ydc/emulators/public/ConZone/nvmev.mod.o
  CC [M]  /home/lab/mnt/home/lab/ydc/emulators/public/ConZone/.module-common.o
  LD [M]  /home/lab/mnt/home/lab/ydc/emulators/public/ConZone/nvmev.ko
  BTF [M] /home/lab/mnt/home/lab/ydc/emulators/public/ConZone/nvmev.ko
make[1]: Leaving directory '/home/lab/mnt/home/lab/ydc/linux/zonelinux-6.12.16'
```
---
### Determine the Command and Launch the Emulator
Since ConZone's flash storage includes user-invisible SLC buffers, emulating a 64GiB SSD requires `64 GiB + [size of SLC buffer] + 1 MiB of memory`. The 1 MiB is for the ZNS SSD emulation in NVMeVirt.

To simplify this, ConZone provides a script, `scripts/size.py`, to help you calculate the sizes.

**Usage**
```Bash
python3 scripts/size.py
```

This will present you with some interactive options. If the default value is suitable, simply press Enter. Note that the options are case-sensitive.

**It is crucial to ensure that the values you enter here match the settings in `ssd_config.h`.**

Here is the meaning of each option:

| Prompt | Corresponding `ssd_config.h` Option  that after`#elif (BASE_SSD == CONZONE_PROTOTYPE)`| Meaning or Influence |
| --- | --- | --- |
| Please enter the prototype of the emulator (conzone or zns):  [default: conzone]:  | None. Corresponds to `# Select one of the targets to build` in `Kbuild`. `CONFIG_NVMEVIRT_CONZONE := y` for conzone; `CONFIG_NVMEVIRT_ZNS := y` for zns. | Influences the minimum SLC buffer size. ZNS is a single-medium type with no SLC buffer; conzone currently defaults to at least 4 SLC superblocks. |
| Please enter the start address of memmap (e.g., 82G):  [default: 82G]:  | None. Corresponds to the starting address of the reserved memory in `/etc/default/grub`. For example, in `GRUB_CMDLINE_LINUX="memmap=79G\\\$82G isolcpus=7,8"`, the start address is 82G. | NVMeVirt needs this to locate the memory for emulation. |
| Please enter the flash type (e.g., TLC, QLC):  [default: QLC]:  | `CELL_MODE` :<br>`CELL_MODE_TLC `: TLC<br>`CELL_MODE_QLC` : QLC | Influences the flash block size, zone size, and zone capacity. Under the ZNS protocol, zone sizes must be powers of 2, a constraint that TLC might violate. |
| Please enter the interface type (e.g., block, zoned):  [default: zoned]:  | `NS_SSD_TYPE_1 `:<br>`SSD_TYPE_CONZONE_BLOCK` : block<br>`SSD_TYPE_CONZONE_ZONED` : zone | Defines the interface type for the data area; the block interface is useful for comparison. |
| Please enter blcok size (e.g., 2M, 32M):  [default: 32M]:  | `BLK_SIZE` ：<br>`MB(24ULL)` : default size of TLC<br>`MB(32ULL)` : default size of QLC | Flash block size. |
| Please enter the number of dies per superblock: [default: 4] | `DIES_PER_ZONE `: Determined by `NAND_CHANNELS` and `LUNS_PER_NAND_CH`, which defaults to 4. | Determines the zone capacity.|
| Please enter the number of pSLC superblocks for data area: [default: 28]: | `DATA_pSLC_INIT_BLKS` ：Defaults to 28, which has no special meaning. | Sets the number of SLC superblocks for the data area (namespace 1). Note that the size of an SLC superblock is 1/3 the size of a TLC superblock and 1/4 the size of a QLC superblock. |
| Please enter the size of data namespace (e.g., 4G):  [default: 4G]: | None. | The user-visible storage size. |
| Please enter the size of meta namespace (e.g., 256M, 40M):  [default: 256M]: | `LOGICAL_META_SIZE` ：An empirical value; see below for how to adjust it.<br>Here are some reference values from my practice:<br>For 4GiB storage, 256MiB for zoned interface, 40MiB for block interface.<br>For 64GiB storage, 512MiB for zoned interface, 376MiB for block interface. | The logical space size for the metadata area, ensuring namespace 0 is exactly the metadata area for F2FS during formatting. |
| Enter the OP ratio for meta area [default: 0.07]: | `OP_AREA_PERCENT` ：Based on NVMeVirt's default of 0.07. | The over-provisioning space for the metadata area and the data area (when using a block interface). It ensures the reserved space is at least 7% of the visible space in the default case. |

Here is the meaning of each output:

| Output | Corresponding `ssd_config.h` Option after `#elif (BASE_SSD == CONZONE_PROTOTYPE)`| Meaning or Influence |
| --- | --- | --- |
| [Zone Size] | `ZONE_SIZE` : Usually doesn't need to be changed; it is `BLK_SIZE * DIES_PER_ZONE`. | The size of a zone. |
| [Super Block Size] | 	None. | The superblock size is usually the same as the Zone Size. |
| [pSLC Flash Block Size] | 	None. | The size of a "Regular" flash block when used for SLC. |
| [pSLC Flash Block Capacity] | None. | The actual usable space after programming a "Regular" flash block as SLC. |
| [Physical Data Size] | 	None.  | 用The user-visible space plus the space occupied by "Regular" flash blocks used for SLC.|
| [Physical Meta Size] | `PHYSICAL_META_SIZE`  | Ensures block alignment and OP requirements are met. It's the size of the "Regular" flash blocks occupied by the meta namespace (since the meta namespace uses SLC, the actual usable physical space is also reduced). |
| Insmod Command |  | The command for launching. |
| [WARN] physical_data_size should be 4104M |  | A warning that the actual user-visible physical space might differ due to block size alignment. |

If you modify `ssd_config.h`, you must recompile:

```bash
make -j `nproc`

```

`ssd_config.h` has an `SLC_BYPASS` variable, which lets you choose whether to bypass the SLC buffer.

* TLC: Usually set `SLC_BYPASS` to 1 (bypass).

* QLC: Must set `SLC_BYPASS` to 0 (do not bypass).

Then, run the provided command. 

```bash
# With all default values
sudo insmod ./nvmev.ko memmap_start=82G memmap_size=8833M cpus=7,8
```

If successful, the command will complete silently. You can then use `lsblk` to see the emulated storage:

```bash
nvme2n1                   259:9    0   256M  0 disk 
nvme2n2                   259:10   0     4G  0 disk 
```

I have added some startup output information that you can view with `sudo dmesg`, for example:

```bash
[2707784.596989] NVMeVirt: Version 1.10 for >> CONZONE SSD Prototype <<
[2707784.597005] NVMeVirt: Storage: 0x1480100000-0x16a8100000 (8832 MiB)
[2707784.621870] NVMeVirt: -------------META PARAMS----------------
[2707784.621874] NVMeVirt: [Logical Space] 256 MiB [Physical Space] 1152 MiB
[2707784.621876] NVMeVirt: [Write Buffer Size] 512 KiB [# of Write Buffer] 1
[2707784.621878] NVMeVirt: [# of pSLC Superblocks] 9 
[2707784.621882] NVMeVirt: [Size of Each Write Buffer] 512 KiB [LPNs per Write Buffer] 128
[2707784.621884] NVMeVirt: ---------zms init block namespace id 0 csi 0 ftl 000000007dcd4479--------------
[2707784.621888] NVMeVirt: ns 0/2: size 256 MiB
[2707784.621890] NVMeVirt: -------------DATA PARAMS (ZONED)----------------
[2707784.621892] NVMeVirt: [Logical Space] 4096 MiB [pSLC Reserved Size] 3584 MiB [Physical Space] 7680 MiB
[2707784.621894] NVMeVirt: [Write Buffer Size] 768 KiB [# of Write Buffer] 6
[2707784.621895] NVMeVirt: [# of pSLC Superblocks] 28 
[2707784.621897] NVMeVirt: [Zone Size] 128 MiB [Zone Capacity] 128 MiB [# of Zones] 32
[2707784.621899] NVMeVirt: [Chunk Size] 4 MiB [Logical Pages per Zone] 32768 [Logical Pages per Chunk] 1024
[2707784.621903] NVMeVirt: [Size of Each Write Buffer] 128 KiB [LPNs per Write Buffer] 32
[2707784.621906] NVMeVirt: ---------zms init zoned namespace id 1 csi 2 ftl 000000004a477c6c--------------
[2707784.621908] NVMeVirt: ns 1/2: size 4096 MiB
[2707784.621911] NVMeVirt: ------------SSD PARAMS-----------
[2707784.621913] NVMeVirt: [# of Channels] 2 [Luns per Channel] 2 [Planes per Lun] 1 [Blocks per Plane] 69 [Logical Pages per Block] 8192
[2707784.621915] NVMeVirt: [Logical Page Size] 4 KiB [Flash Page Size] 32 KiB [Oneshot Page Size] 128 KiB
[2707784.621917] NVMeVirt: [Total pSLC Superblocks] 37 [Meta pSLC Superblocks] 9 [Meta Normal Superblocks] 0
[2707784.621919] NVMeVirt: [Total pSLC Size] 4736 MiB [Meta pSLC Size] 1152 MiB
[2707784.621921] NVMeVirt: [Total pSLC Capacity] 1184 MiB [Meta pSLC Capacity] 288 MiB
[2707784.621922] NVMeVirt: [Logical Pages per pSLC Block] 2048 [Logical Pages per pSLC Line] 8192
[2707784.621924] NVMeVirt: Total Capacity(GiB,MiB)=8,8832 chs=2 luns=4 lines=69 blk-size(MiB,KiB)=32,32768 line-size(MiB,KiB)=128,131072
[2707784.621928] NVMeVirt: ----------------------------
[2707784.689711] NVMeVirt: [chmodel_init] bandwidth 1600 max_credits 52 tx_time 76
[2707784.754781] NVMeVirt: [chmodel_init] bandwidth 1600 max_credits 52 tx_time 76
[2707784.754807] NVMeVirt: [chmodel_init] bandwidth 3000 max_credits 98 tx_time 40
[2707784.756210] NVMeVirt: [# of L2P Entries (cached/all)] 261120 / 65536 [Evict Policy] 1 [Pre Read Pages] 0
[2707784.756218] NVMeVirt: pqueue: Copyright (c) 2014, Volkan Yazıcı <volkan.yazici@gmail.com>. All rights reserved.
[2707784.756222] NVMeVirt: Line id 0 -> Block id 0 nand type 1
[2707784.756224] NVMeVirt: Line id 1 -> Block id 1 nand type 1
[2707784.756225] NVMeVirt: Line id 2 -> Block id 2 nand type 1
[2707784.756227] NVMeVirt: Line id 3 -> Block id 3 nand type 1
[2707784.756228] NVMeVirt: Line id 4 -> Block id 4 nand type 1
[2707784.756230] NVMeVirt: Line id 5 -> Block id 5 nand type 1
[2707784.756231] NVMeVirt: Line id 6 -> Block id 6 nand type 1
[2707784.756233] NVMeVirt: Line id 7 -> Block id 7 nand type 1
[2707784.756234] NVMeVirt: Line id 8 -> Block id 8 nand type 1
[2707784.756236] NVMeVirt: Line Management: [# of Top Level Lines] 9 [Interleave Lines] 9 [pSLC Lines] 9
[2707784.756238] NVMeVirt: Line Management: [Free Normal Lines] 0 [Free pSLC Lines] 9
[2707784.756240] NVMeVirt: prepared slc wp for USER IO, line id 0 blk id 0
[2707784.756242] NVMeVirt: prepared slc wp for GC IO, line id 1 blk id 1
[2707784.756244] NVMeVirt: [Total Lines] 9 [pSLC Lines] 9 [Normal lines] 0
[2707784.756844] NVMeVirt: [# of RMap Entries] 294912
[2707784.756848] NVMeVirt: [Num Aggs] 1
[2707784.756850] NVMeVirt: --------------- realize block namespace 0 ssd 0000000002f829ab--------------
[2707784.760462] NVMeVirt: [# of L2P Entries (cached/all)] 261120 / 1048576 [Evict Policy] 1 [Pre Read Pages] 0
[2707784.760473] NVMeVirt: Line id 0 -> Block id 9 nand type 1
[2707784.760475] NVMeVirt: Line id 1 -> Block id 10 nand type 1
... [Truncated: line and block mapping information]
[2707784.760561] NVMeVirt: Line Management: [# of Top Level Lines] 60 [Interleave Lines] 60 [pSLC Lines] 28
[2707784.760563] NVMeVirt: Line Management: [Free Normal Lines] 32 [Free pSLC Lines] 28
[2707784.760565] NVMeVirt: prepared normal wp for USER IO, line id 28 blk id 37
[2707784.760567] NVMeVirt: prepared slc wp for USER IO, line id 0 blk id 9
[2707784.760568] NVMeVirt: prepared slc wp for GC IO, line id 1 blk id 10
[2707784.760570] NVMeVirt: [Total Lines] 60 [pSLC Lines] 28 [Normal lines] 32
[2707784.764567] NVMeVirt: [# of RMap Entries] 1966080
[2707784.764576] NVMeVirt: [Num Aggs] 32
[2707784.764578] NVMeVirt: --------------- realize zoned namespace 1 ssd 0000000002f829ab--------------
[2707784.764651] PCI host bridge to bus 0001:10
[2707784.764655] pci_bus 0001:10: root bus resource [io  0x0000-0xffff]
[2707784.764659] pci_bus 0001:10: root bus resource [mem 0x00000000-0x3fffffffffff]
[2707784.764663] pci_bus 0001:10: root bus resource [bus 00-ff]
[2707784.764674] pci 0001:10:00.0: [0c51:0110] type 00 class 0x010802 PCIe Endpoint
[2707784.764680] pci 0001:10:00.0: BAR 0 [mem 0x1480000000-0x1480003fff 64bit]
[2707784.764685] pci 0001:10:00.0: enabling Extended Tags
[2707784.765315] NVMeVirt: Virtual PCI bus created (node 0)
[2707784.765952] NVMeVirt: nvmev_io_worker_0 started on cpu 8 (node 0)
[2707784.766187] NVMeVirt: nvmev_dispatcher started on cpu 7 (node 0)
[2707784.766535] nvme nvme2: pci function 0001:10:00.0
[2707784.770234] nvme nvme2: 1/0/0 default/read/poll queues
[2707784.793018] NVMeVirt: Virtual NVMe device created
```

### Formatting to F2FS

ConZone's code includes modified `mkfs.tool` source to help you determine the logical size of namespace 0.

If you are unsure how much space to allocate for metadata, you can first assume it is twice the size of your zone. Then, run the following command with `sudo`:

```bash
 sudo ./f2fs-tools-1.14.0/build/sbin/mkfs.f2fs -f -m -c /dev/nvme2n2 /dev/nvme2n1
```

P.S. The `-m` parameter is not needed for formatting a block interface SSD.

The output will look something like this:

```bash
        F2FS-tools: mkfs.f2fs Ver: 1.14.0 (2020-08-24)

Info: Disable heap-based policy
Info: Debug level = 0
Info: Trim is enabled
Info: Host-managed zoned block device:
      32 zones, 0 randomly writeable zones
      32768 blocks per zone
Info: Segments per section = 64
Info: Sections per zone = 1
Info: sector size = 512
Info: total sectors = 8912896 (4352 MB)
Info: zone aligned segment0 blkaddr: 32768
...
        [MISAO] Info: device 0 /dev/nvme2n1 blkaddr[0 - 65535] tt segs 64 
        [MISAO] Info: device 1 /dev/nvme2n2 blkaddr[65536 - 1114111] tt segs 2048 
     ...
        [MISAO] Info: main blkaddr 65536
```

You can see that “main blkaddr” and the starting position of “device 1“ are the same, which means it was successful.

If the size you set is too large, reduce `LOGICAL_META_SIZE` by one zone. If it's too small, increase it by one zone. Keep adjusting until it is correct.

Note: After adjusting, you must rerun `scripts/size.py`, check if other parameters have changed, recompile, run with the generated command, and then reformat with the tool.

### Mounting

Mount the meta namespace disk.

```bash
# mnt is my custom mount directory
sudo mount /dev/nvme2n1 mnt
```

### Test and Analysis

Now, you can test its performance using tools like FIO, Mobibench, or others, just like any regular disk.

For batch testing, I've created the `exp.py` script (for a complete batch test), `mount.py` and `umount.py` scripts (for individual mount/unmount operations, useful for single tests) in the `scripts/` directory. Remember to modify `WORK_DIR` and `INSMOD_CMD` in these scripts.

(P.S. I have also included the Mobibench source code in the repository, which may be helpful!)


### Unmounting

```bash
sudo umount mnt
sudo rmmod nvmev
```

### Viewing Statistics

I have added some potentially useful statistics that you can view with `sudo dmesg`, for example:

```bash
[2709034.940583] NVMeVirt: -------------MISAO SSD statistic info-----------
[2709034.940593] NVMeVirt: [Channel 0 Lun 0] [Max CMD Queue Depth] 4
[2709034.940601] NVMeVirt: [Channel 0 Lun 1] [Max CMD Queue Depth] 4
[2709034.940607] NVMeVirt: [Channel 1 Lun 0] [Max CMD Queue Depth] 4
[2709034.940613] NVMeVirt: [Channel 1 Lun 1] [Max CMD Queue Depth] 4
[2709035.119993] NVMeVirt: ------------MISAO--device 0 statistic info-----------
[2709035.119997] NVMeVirt: [# of Zone Resets] 0 [# of Zone Writes] 0
[2709035.119999] NVMeVirt: [# of Host Read Requests] 161
[2709035.120001] NVMeVirt: [# of Host Write Requests] 62
[2709035.120002] NVMeVirt: [# of Host Flush Requests] 4
[2709035.120004] NVMeVirt: [Host Read Pages] 846
[2709035.120006] NVMeVirt: [L2P Miss Rate] (0/63) [WB Hits] 0 [Unmapped Read Cnt] 783
[2709035.120008] NVMeVirt: [WAF] (3104/3104) [Migrated Logical Pages] 0
[2709035.120010] NVMeVirt: [# of Normal Line Earse] 0 [# of pSLC Line Erase] 0
[2709035.120012] NVMeVirt: [# of Garbage Collection] 0
[2709035.120013] NVMeVirt: [# of Early Flush] 4
[2709035.120015] NVMeVirt: [pSLC lines] 7/0/0/9 [normal lines] 0/0/0/0 (free/full/victim/all)
[2709035.120017] NVMeVirt: [# of inplace update] 279
[2709035.120833] NVMeVirt: ------------MISAO--device 1 statistic info-----------
[2709035.120837] NVMeVirt: [# of Zone Resets] 0 [# of Zone Writes] 0
[2709035.120839] NVMeVirt: [# of Host Read Requests] 104
[2709035.120841] NVMeVirt: [# of Host Write Requests] 2
[2709035.120843] NVMeVirt: [# of Host Flush Requests] 1
[2709035.120845] NVMeVirt: [Host Read Pages] 530
[2709035.120847] NVMeVirt: [L2P Miss Rate] (0/3) [WB Hits] 0 [Unmapped Read Cnt] 527
[2709035.120849] NVMeVirt: [WAF] (2/2) [Migrated Logical Pages] 0
[2709035.120851] NVMeVirt: [# of Normal Line Earse] 0 [# of pSLC Line Erase] 0
[2709035.120853] NVMeVirt: [# of Garbage Collection] 0
[2709035.120855] NVMeVirt: [# of Early Flush] 2
[2709035.120857] NVMeVirt: [pSLC lines] 26/0/0/28 [normal lines] 31/0/0/32 (free/full/victim/all)
[2709035.120860] NVMeVirt: [# of inplace update] 0
[2709035.144637] NVMeVirt: Virtual NVMe device closed
```


## License

NVMeVirt is offered under the terms of the GNU General Public License version 2 as published by the Free Software Foundation. More information about this license can be found [here](https://www.gnu.org/licenses/old-licenses/gpl-2.0.en.html).

Priority queue implementation [`pqueue/`](pqueue/) is offered under the terms of the BSD 2-clause license (GPL-compatible). (Copyright (c) 2014, Volkan Yazıcı <volkan.yazici@gmail.com>. All rights reserved.)
