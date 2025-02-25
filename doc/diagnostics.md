## Cache

```shell
$ lscpu | grep -A 3 L1d
L1d cache:                            192 KiB (4 instances)
L1i cache:                            128 KiB (4 instances)
L2 cache:                             5 MiB (4 instances)
L3 cache:                             12 MiB (1 instance

$ cat /sys/devices/system/cpu/cpu0/cache/index0/size
48K
$ cat /sys/devices/system/cpu/cpu0/cache/index0/number_of_sets 
64
$ cat /sys/devices/system/cpu/cpu0/cache/index0/coherency_line_size 
64
$ cat /sys/devices/system/cpu/cpu0/cache/index0/ways_of_associativity 
12
```

## Page size (RAM)

```shell
$ getconf PAGESIZE
4096
```

## SSD
```shell
$ cat /sys/block/nvme0n1/queue/minimum_io_size 
512
$ cat /sys/block/nvme0n1/queue/optimal_io_size 
0
$ cat /sys/block/nvme0n1/queue/physical_block_size
512
$ cat /sys/class/mtd/mtd0/erasesize 
4096
$ cat /sys/block/nvme0n1/queue/erase_size

sudo apt update && sudo apt install smartmontools
sudo smartctl -x /dev/sdX
sudo smartctl -x /dev/nvme0n1

```

@TODO Im looging for a way to get erase block size.