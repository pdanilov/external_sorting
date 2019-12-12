# External merge sorting
This repo provides util for lexicographical file contents sorting for file sizes
potentially too big to ensure fitting into RAM

Pre-requisites:
- python >= 3.7
- tqdm (optional, but strongly recommended, for progress-bars in both scripts)

Example of usage:
```
python generate.py --file 1KK.txt --num_lines 1000000 --max_length 1000
python sort.py --file 1KK.txt --dst_file 1KK_sorted.txt --max_length 1000 --ram_capacity 8G
```
In this case the sort script expects that it can use 8 gigs of RAM, that's true for most of modern computers,
you can also provide something like 0.5G or 500M

Also note that you should provide `max_length` argument not only for generator script, but also for sort script:
this value is used for computing, how many words the script can load into RAM