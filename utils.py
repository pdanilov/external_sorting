import os
from typing import TextIO, Iterable


def read_lines_lazy(file: TextIO):
    while True:
        line = file.readline().strip('\n')
        if line:
            yield line
        else:
            break


def write_with_sep(file: TextIO, iterable: Iterable, sep='\n'):
    for line in iterable:
        file.write(line + sep)
    file.seek(0)


def remove_temporary(file: TextIO):
    file.close()
    os.remove(file.name)
