from argparse import ArgumentParser
from dataclasses import dataclass, field
from queue import PriorityQueue
from tempfile import NamedTemporaryFile
from typing import Any, List, TextIO

from tqdm import tqdm

from utils import get_logger_with_console_handler, read_lines_lazy, write_with_sep, remove_temporary

logger = get_logger_with_console_handler(__name__)


@dataclass(order=True)
class PriorityQueueItem:
    line: str
    it: Any = field(compare=False)


@dataclass
class SourceFileStats:
    files: List[TextIO]
    num_lines: int


def write_lines_to_temporary(lines: List[str]) -> TextIO:
    file = NamedTemporaryFile(delete=False, mode='w+')
    write_with_sep(file, lines)
    return file


def split_file_into_sorted_chunks(file: TextIO, chunk_size: int) -> SourceFileStats:
    files = []
    lines = []
    idx = -1

    logger.info("Reading input file, splitting into chunks, that fit into RAM, sorting, then writing on disk.")

    for idx, line in tqdm(enumerate(read_lines_lazy(file))):
        lines.append(line)
        if len(lines) == chunk_size:
            tmp_file = write_lines_to_temporary(sorted(lines))
            files.append(tmp_file)
            lines = []

    if len(lines) > 0:
        tmp_file = write_lines_to_temporary(sorted(lines))
        files.append(tmp_file)

    return SourceFileStats(files, idx+1)


def merge_by_priority_queue(queue: PriorityQueue) -> TextIO:
    while not queue.empty():
        item = queue.get_nowait()
        line, it = item.line, item.it
        yield line

        try:
            line = next(it)
        except StopIteration:
            continue
        else:
            item = PriorityQueueItem(line, it)
            queue.put_nowait(item)


def priority_queue_from_chunk_files(chunk_files: List[TextIO]) -> PriorityQueue:
    queue = PriorityQueue()
    for file in chunk_files:
        it = read_lines_lazy(file)
        line = next(it)
        item = PriorityQueueItem(line, it)
        queue.put_nowait(item)
    return queue


def merge(file: TextIO, chunk_files: List[TextIO], num_lines: int):
    queue = priority_queue_from_chunk_files(chunk_files)
    merged = merge_by_priority_queue(queue)
    logger.info("Merging chunks into one file.")
    merged = tqdm(merged, total=num_lines)
    write_with_sep(file, merged)

    for chunk_file in chunk_files:
        remove_temporary(chunk_file)


def external_merge_sort(file: TextIO, dst_file: TextIO, max_length: int, available_memory: int):
    # the size of every string is (49 + len) bytes, generated strings are not all with len == 'max_length'
    # so asymptotically number of strings that may be load into RAM is equal to 'available_memory' / 'max_length'
    chunk_size = available_memory // max_length
    stats = split_file_into_sorted_chunks(file, chunk_size)
    merge(dst_file, stats.files, stats.num_lines)


def _parse_args():
    parser = ArgumentParser()
    parser.add_argument('--file', type=str, required=True,
                        help='source file')
    parser.add_argument('--dst_file', type=str, required=True,
                        help='destination file with sorted data')
    parser.add_argument('--max_length', type=int, required=True,
                        help='maximal length of line in source file')
    parser.add_argument('--ram_capacity', type=str, default='4G',
                        help='amount RAM used for algorithm')
    return parser.parse_args()


def _parse_memory_str(memory_str: str) -> int:
    vol, unit = float(memory_str[:-1]), memory_str[-1].upper()

    if unit == 'B':
        shift = 0
    elif unit == 'K':
        shift = 10
    elif unit == 'M':
        shift = 20
    elif unit == 'G':
        shift = 30
    else:
        raise ValueError("Unrecognized unit identifier: '{}', must be 'B', 'K', 'M' or 'G'".format(unit))

    vol *= (1 << shift)
    return int(vol)


def main():
    args = _parse_args()
    available_memory = _parse_memory_str(args.ram_capacity)

    with open(args.file, mode='r') as file:
        with open(args.dst_file, mode='w') as dst_file:
            external_merge_sort(file, dst_file, args.max_length, available_memory)


if __name__ == '__main__':
    main()
