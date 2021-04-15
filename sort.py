from argparse import ArgumentParser
from collections import namedtuple
from itertools import accumulate
import math
import shutil
from tempfile import NamedTemporaryFile
from typing import IO, Optional

from tqdm import trange

Pair = namedtuple('Pair', ['first', 'second'])


class FileIterator:
    def __init__(self, file: IO, start_offset: int, num_lines_to_process: int):
        self.file = file
        self.file.seek(start_offset)
        self.line = None
        self.line_idx = 0
        self.num_lines_to_process = num_lines_to_process

    def readline(self):
        self.line = self.file.readline()
        self.line_idx += 1

    def is_valid(self) -> bool:
        return self.line_idx <= self.num_lines_to_process


def merge_sort(
    in_file: IO,
    out_file: IO,
    chunk_size: int,
    use_tqdm: Optional[bool] = True,
):
    f = NamedTemporaryFile(mode='w+b')
    in_file_pair = Pair(first=f, second=open(f.name, mode=f.mode))
    f = NamedTemporaryFile(mode='w+b')
    out_file_pair = Pair(first=f, second=open(f.name, mode=f.mode))

    in_file.seek(0)
    shutil.copyfileobj(in_file, in_file_pair.first)
    in_file.seek(0)
    num_lines = sum(1 for _ in in_file)
    max_pow = int(math.log(num_lines, 2))
    iterable = trange(max_pow+1) if use_tqdm else range(max_pow+1)

    for power in iterable:
        width = 1 << power
        in_file_pair.first.seek(0)
        offsets = [len(line) for line in in_file_pair.first]
        offsets.insert(0, 0)
        offsets = [*accumulate(offsets)]

        lhs_range = range(0, num_lines, 2*width)

        rhs_range = range(width, num_lines+width, 2*width)
        rhs_range = map(lambda x: min(x, num_lines), rhs_range)

        end_range = range(2*width, num_lines+2*width, 2*width)
        end_range = map(lambda x: min(x, num_lines), end_range)

        for lhs_start_idx, rhs_start_idx, end_idx in zip(
            lhs_range, rhs_range, end_range
        ):
            lhs = FileIterator(
                in_file_pair.first,
                offsets[lhs_start_idx],
                rhs_start_idx-lhs_start_idx,
            )
            rhs = FileIterator(
                in_file_pair.second,
                offsets[rhs_start_idx],
                end_idx-rhs_start_idx,
            )
            out = FileIterator(
                out_file_pair.first,
                offsets[lhs_start_idx],
                end_idx-lhs_start_idx,
            )
            bottom_up_merge(
                lhs, rhs, out, chunk_size=chunk_size,
            )

        in_file_pair, out_file_pair = out_file_pair, in_file_pair

    in_file_pair.first.seek(0)
    shutil.copyfileobj(in_file_pair.first, out_file)

    for f in (
        in_file_pair.first,
        in_file_pair.second,
        out_file_pair.first,
        out_file_pair.second,
    ):
        f.close()


def bottom_up_merge(
    lhs: FileIterator,
    rhs: FileIterator,
    out: FileIterator,
    chunk_size: int,
):
    chunk = []
    lhs.readline()
    rhs.readline()

    for out_idx in range(out.num_lines_to_process):
        lhs_is_available = lhs.is_valid()
        rhs_is_available = rhs.is_valid()
        lhs_lt_rhs = lhs.line < rhs.line

        if lhs_is_available and (not rhs_is_available or lhs_lt_rhs):
            out_line = lhs.line
            lhs.readline()
        else:
            out_line = rhs.line
            rhs.readline()

        chunk.append(out_line)

        if len(chunk) == chunk_size:
            out.file.write(b''.join(chunk))
            out.file.flush()
            chunk.clear()

    if chunk:
        out.file.write(b''.join(chunk))
        out.file.flush()
        chunk.clear()


def parse_args():
    parser = ArgumentParser()
    parser.add_argument(
        'infile', type=str,
        help='input file',
    )
    parser.add_argument(
        'outfile', type=str,
        help='output file with sorted data',
    )
    parser.add_argument(
        '--chunk-size', type=int, default=10_000,
        help='chunk size used for writing into file',
    )
    parser.add_argument(
        '--workers', type=int, default=None,
        help='number of workers used, omit this one to use all CPU cores',
    )
    parser.add_argument(
        '--no-tqdm', dest='use_tqdm', action='store_false',
        help='don\'t use tqdm',
    )
    return parser.parse_args()


def main():
    args = parse_args()
    in_file = open(args.infile, mode='rb')
    out_file = open(args.outfile, mode='wb')
    merge_sort(
        in_file,
        out_file,
        args.chunk_size,
        use_tqdm=args.use_tqdm,
    )
    in_file.close()
    out_file.close()


if __name__ == '__main__':
    main()
