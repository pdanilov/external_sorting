from argparse import ArgumentParser
from itertools import accumulate
import math
import shutil
from tempfile import TemporaryFile
from typing import IO, List, Tuple

from tqdm import trange


def swap_files(in_file: IO, out_file: IO) -> Tuple[IO, IO]:
    in_file.seek(0)
    out_file.seek(0)
    in_file.truncate(0)
    return out_file, in_file


def merge_sort(in_file: IO[str], out_file: IO[str]):
    in_file_tmp = TemporaryFile(mode='w+')
    out_file_tmp = TemporaryFile(mode='w+')

    in_file.seek(0)
    shutil.copyfileobj(in_file, in_file_tmp)
    in_file_tmp.seek(0)
    num_lines = sum(1 for _ in in_file_tmp)

    max_pow = int(math.log(num_lines, 2))
    width = 1
    for _ in trange(0, max_pow+1):
        in_file_tmp.seek(0)
        offsets = [len(line) for line in in_file_tmp]
        offsets.insert(0, 0)
        offsets = [*accumulate(offsets)]
        for i in range(0, num_lines, 2*width):
            lhs_start_idx = i
            rhs_start_idx = min(i+width, num_lines)
            end_idx = min(i+2*width, num_lines)
            bottom_up_merge(
                in_file_tmp,
                out_file_tmp,
                offsets,
                lhs_start_idx,
                rhs_start_idx,
                end_idx,
            )
        in_file_tmp, out_file_tmp = swap_files(in_file_tmp, out_file_tmp)
        width *= 2

    shutil.copyfileobj(in_file_tmp, out_file)
    in_file_tmp.close()
    out_file_tmp.close()


def bottom_up_merge(
    in_file: IO[str],
    out_file: IO[str],
    offsets: List[int],
    lhs_start_idx: int,
    rhs_start_idx: int,
    end_idx: int,
):
    lhs_idx, rhs_idx = lhs_start_idx, rhs_start_idx
    in_file.seek(offsets[lhs_idx])
    lhs_line = in_file.readline()
    in_file.seek(offsets[rhs_idx])
    rhs_line = in_file.readline()

    for out_idx in range(lhs_start_idx, end_idx):
        lhs_is_available = lhs_idx < rhs_start_idx
        rhs_is_available = rhs_idx < end_idx
        lhs_lt_rhs = lhs_line < rhs_line

        if lhs_is_available and (not rhs_is_available or lhs_lt_rhs):
            out_line = lhs_line
            lhs_idx += 1
            in_file.seek(offsets[lhs_idx])
            lhs_line = in_file.readline()
        else:
            out_line = rhs_line
            rhs_idx += 1
            in_file.seek(offsets[rhs_idx])
            rhs_line = in_file.readline()

        out_file.write(out_line)


def parse_args():
    parser = ArgumentParser()
    parser.add_argument('in_file', type=str,
                        help='input file')
    parser.add_argument('out_file', type=str,
                        help='output file with sorted data')
    return parser.parse_args()


def main():
    args = parse_args()

    in_file = open(args.in_file, mode='r')
    out_file = open(args.out_file, mode='w')
    merge_sort(in_file, out_file)
    in_file.close()
    out_file.close()


if __name__ == '__main__':
    main()
