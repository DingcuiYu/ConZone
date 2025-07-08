# ConZone

## Introduction

ConZone is a versatile software-defined consumer-level virtual zoned device. It is developed based on [NVMeVirt](https://github.com/snu-csl/nvmevirt), which is implemented as a Linux kernel module.

Further details on the design and implementation of ConZone can be found in the following papers.

Please feel free to contact us at [xxx@gamil.com] if you have any questions or suggestions.

## ConZone Installation

### Linux kernel requirement

The recommended Linux kernel version is v5.15.x and higher (tested on Ubuntu kernel 6.8.0-45-generic).

### Reserving physical memory

A part of the main memory should be reserved for the storage of the emulated NVMe device. To reserve a chunk of physical memory, add the following option to `GRUB_CMDLINE_LINUX` in `/etc/default/grub` as follows:

```bash
GRUB_CMDLINE_LINUX="memmap=12G\\\$80G"
```

This example will reserve 12GiB of physical memory chunk (out of the total 93GiB physical memory) starting from the 80GiB memory offset. You may need to adjust those values depending on the available physical memory size and the desired storage capacity. The available continuous memory can be checked by running `sudo cat /proc/iomem`.

Additionally, it is highly recommended to use the `isolcpus` Linux command-line configuration to avoid schedulers putting tasks on the CPUs that NVMeVirt uses:

```bash
GRUB_CMDLINE_LINUX="memmap=12G\\\$80G isolcpus=7,8"
```

After changing the `/etc/default/grub` file, you are required to run the following commands to update `grub` and reboot your system.

```bash
$ sudo update-grub
$ sudo reboot
```

### Compiling `conzone`

Please download the latest version of `conzone` from Github:

```bash
$ git clone https://github.com/xxx
```

Since `conzone` is based on `nvmevirt`, which is implemented as a Linux kernel module. Thus, the kernel headers should be installed in the `/lib/modules/$(shell uname -r)` directory to compile `conzone`.

Currently, we add a new target device type named `ZMS` to distinguish with enterprise-level zoned device `ZNS`. The corresponding compilation options have been modified.

You may find the detailed configuration parameters for current `conzone` from `ssd_config.h` in the parameters of `ZMS_PROTOTYPE`.

Build the kernel module by running the `make` command in the `conzone` source directory. You can accelerate compilation by using `-j` option.
```bash
$ make -j `nproc`
make -C /lib/modules/6.8.0-45-generic/build M=/path/to/nvmev modules
make[1]: Entering directory '/usr/src/linux-headers-6.8.0-45-generic'
  CC [M]  /path/to/nvmev/main.o
  CC [M]  /path/to/nvmev/pci.o
  CC [M]  /path/to/nvmev/admin.o
  CC [M]  /path/to/nvmev/io.o
  CC [M]  /path/to/nvmev/ssd.o
  CC [M]  /path/to/nvmev/zns_ftl.o
  CC [M]  /path/to/nvmev/zns_read_write.o
  CC [M]  /path/to/nvmev/zms_read_write.o
  CC [M]  /path/to/nvmev/zns_mgmt_send.o
  CC [M]  /path/to/nvmev/zns_mgmt_recv.o
  CC [M]  /path/to/nvmev/channel_model.o
  CC [M]  /path/to/nvmev/conv_ftl.o
  CC [M]  /path/to/nvmev/simple_ftl.o
  CC [M]  /path/to/nvmev/pqueue/pqueue.o
  LD [M]  /path/to/nvmev/nvmev.o
  MODPOST /path/to/nvmev/Module.symvers
  CC [M]  /path/to/nvmev/nvmev.mod.o
  LD [M]  /path/to/nvmev/nvmev.ko
  BTF [M] /path/to/nvmev/nvmev.ko
Skipping BTF generation for /path/to/nvmev/nvmev.ko due to unavailability of vmlinux
make[1]: Leaving directory '/usr/src/linux-headers-6.8.0-45-generic'
$
```

### Using `conzone`

META SPACE
TODO!!!

You can attach an emulated consumer-level zoned storage in your system by loading the kernel module as follows:

```bash
$ sudo insmod ./nvmev.ko \
  memmap_start=128G \       # e.g., 1M, 4G, 8T
  memmap_size=64G   \       # e.g., 1M, 4G, 8T
  cpus=7,8                  # List of CPU cores to process I/O requests (should have at least 2)
```

In the above example, `memmap_start` and `memmap_size` indicate the relative offset and the size of the reserved memory, respectively. Those values should match the configurations specified in the `/etc/default/grub` file shown earlier. In addition, the `cpus` option specifies the id of cores on which I/O dispatcher and I/O worker threads run. You have to specify at least two cores for this purpose: one for the I/O dispatcher thread, and one or more cores for the I/O worker thread(s).

It is highly recommended to use the `isolcpus` Linux command-line configuration to avoid schedulers putting tasks on the CPUs that NVMeVirt uses:

```bash
GRUB_CMDLINE_LINUX="memmap=64G\\\$128G isolcpus=7,8"
```

When you are successfully load the `nvmevirt` module, you can see something like these from the system message.

```log
$ sudo dmesg
[  144.812917] nvme nvme0: pci function 0001:10:00.0
[  144.812975] NVMeVirt: Successfully created virtual PCI bus (node 1)
[  144.813911] NVMeVirt: nvmev_proc_io_0 started on cpu 7 (node 1)
[  144.813972] NVMeVirt: Successfully created Virtual NVMe device
[  144.814032] NVMeVirt: nvmev_dispatcher started on cpu 8 (node 1)
[  144.822075] nvme nvme0: 48/0/0 default/read/poll queues
```

If you encounter a kernel panic in `__pci_enable_msix()` or in `nvme_hwmon_init()` during `insmod`, it is because the current implementation of `nvmevirt` is not compatible with IOMMU. In this case, you can either turn off Intel VT-d or IOMMU in BIOS, or disable the interrupt remapping using the grub option as shown below:

```bash
GRUB_CMDLINE_LINUX="memmap=64G\\\$128G intremap=off"
```

Now the emulated `nvmevirt` device is ready to be used as shown below. The actual device number (`/dev/nvme0`) can vary depending on the number of real NVMe devices in your system.


```bash
$ ls -l /dev/nvme*
crw------- 1 root root 242, 0 Feb 22 14:13 /dev/nvme0
brw-rw---- 1 root disk 259, 5 Feb 22 14:13 /dev/nvme0n1
```


## License

NVMeVirt is offered under the terms of the GNU General Public License version 2 as published by the Free Software Foundation. More information about this license can be found [here](https://www.gnu.org/licenses/old-licenses/gpl-2.0.en.html).

Priority queue implementation [`pqueue/`](pqueue/) is offered under the terms of the BSD 2-clause license (GPL-compatible). (Copyright (c) 2014, Volkan Yazıcı <volkan.yazici@gmail.com>. All rights reserved.)
