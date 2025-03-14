## Syscalls

```shell
real	2m32,892s
user	1m30,430s
sys	  0m9,041s


$ strace -c -e trace=mmap,brk,open,close,write,read,lseek -o strace.log ./example
$ cat strace.log 
% time     seconds  usecs/call     calls    errors syscall
------ ----------- ----------- --------- --------- ----------------
 66,65   20,001627           1  14046666           read
 27,25    8,178276           1   7224026           lseek
  4,36    1,307611          64     20236           write
  1,72    0,517506         105      4885           close
  0,02    0,005543           4      1317           brk
  0,00    0,000053           2        22           mmap
------ ----------- ----------- --------- --------- ----------------
100,00   30,010616           1  21297152           total

% time     seconds  usecs/call     calls    errors syscall
------ ----------- ----------- --------- --------- ----------------
 66,73   20,451427           1  14046690           read
 27,07    8,297533           1   7224050           lseek
  4,43    1,356546          67     20238           write
  1,75    0,536461         109      4885           close
  0,03    0,008153           4      1919           brk
  0,00    0,000056           2        22           mmap
------ ----------- ----------- --------- --------- ----------------
100,00   30,650176           1  21297804           total
```

## RocksDB

```shell
real	0m27,838s
user	0m29,984s
sys	  0m1,689s

% time     seconds  usecs/call     calls    errors syscall
------ ----------- ----------- --------- --------- ----------------
 99,60    0,502698           1    500124           write
  0,27    0,001347           0      2242           brk
  0,10    0,000520           6        84           mmap
  0,02    0,000078           1        60           close
  0,01    0,000067           1        34           read
  0,00    0,000000           0         1           lseek
------ ----------- ----------- --------- --------- ----------------
100,00    0,504710           1    502545           total

% time     seconds  usecs/call     calls    errors syscall
------ ----------- ----------- --------- --------- ----------------
 99,53    0,383944           0    500125           write
  0,29    0,001130           0      2251           brk
  0,12    0,000460           5        84           mmap
  0,04    0,000136           2        60           close
  0,02    0,000073           2        34           read
  0,00    0,000000           0         1           lseek
------ ----------- ----------- --------- --------- ----------------
100,00    0,385743           0    502555           total


```










master:
  ```shell
% time     seconds  usecs/call     calls    errors syscall
------ ----------- ----------- --------- --------- ----------------
 73,84  146,450707           2  72042446           read
 21,95   43,538478           1  25087476           lseek
  3,62    7,174942           5   1320064           write
  0,59    1,172235         120      9768           close
  0,00    0,003295           2      1358           brk
  0,00    0,000225          11        19           mmap
------ ----------- ----------- --------- --------- ----------------
100,00  198,339882           2  98461131           total

real	21m26,241s
user	1m53,795s
sys	  15m15,758s
 ```

PR:
 ```shell
% time     seconds  usecs/call     calls    errors syscall
------ ----------- ----------- --------- --------- ----------------
 71,30    2,846802           2   1285726           read
 21,49    0,857810           1    613793           lseek
  4,83    0,192688          67      2869           write
  2,00    0,079792          40      1950           close
  0,38    0,015166          11      1294           brk
  0,01    0,000300          15        19           mmap
------ ----------- ----------- --------- --------- ----------------
100,00    3,992558           2   1905651           total

real	0m59,433s
user	0m5,249s
sys	  0m17,802s
 ```