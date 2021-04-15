from argparse import ArgumentParser
from concurrent.futures import ProcessPoolExecutor
from functools import partial
import random
import string
from typing import IO, List, Optional

from tqdm import tqdm


def random_string(length: int, *, chars: List[bytes]) -> bytes:
    line = b''.join(random.choices(chars, k=length))
    return line


def write_lines_to_file(
    file: IO,
    num_lines: int,
    max_length: int,
    chars: Optional[str] = None,
    workers: Optional[int] = None,
    use_tqdm: Optional[bool] = True,
):
    if not chars:
        chars = string.printable

    for char in string.whitespace:
        if char in ' \t':
            continue
        chars = chars.replace(char, '')

    chars = [char.encode() for char in chars]
    line_lens = random.choices([*range(1, max_length+1)], k=num_lines)

    with ProcessPoolExecutor(workers) as executor:
        mapping = executor.map(partial(random_string, chars=chars),
                               line_lens,
                               chunksize=100)
        if use_tqdm:
            mapping = tqdm(mapping, total=num_lines)

        for line in mapping:
            file.write(line)
            file.write(b'\n')


def parse_args():
    parser = ArgumentParser()
    parser.add_argument(
        'path', type=str,
        help='destination file path to write',
    )
    parser.add_argument(
        '--num-lines', type=int, required=True,
        help='number of lines in generated file',
    )
    parser.add_argument(
        '--max-length', type=int, required=True,
        help='max length of lines',
    )
    parser.add_argument(
        '--charset', type=str,
        choices=[
            'whitespace',
            'ascii_lowercase',
            'ascii_uppercase',
            'ascii_letters',
            'digits',
            'hexdigits',
            'octdigits',
            'punctuation',
            'printable',
        ],
        help='alphabet of characters to use',
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
    chars = getattr(string, args.charset) if args.charset else args.charset
    with open(args.path, mode='wb') as file:
        write_lines_to_file(
            file,
            args.num_lines,
            args.max_length,
            chars=chars,
            workers=args.workers,
            use_tqdm=args.use_tqdm,
        )


if __name__ == '__main__':
    main()
