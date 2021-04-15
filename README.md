# External merge sorting
This repo provides util for lexicographical file contents sorting for file sizes
potentially too big to ensure fitting into RAM

Pre-requisites:
- python >= 3.5 (for type hints support)
- tqdm

Example of usage:
```
python generate.py 1KK.txt --num-lines 1000000 --max-length 1000
python sort.py 1KK.txt 1KK_sorted.txt
```
