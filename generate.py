from argparse import ArgumentParser
from functools import partial
import multiprocessing as mp
import random
import string
from typing import IO, Optional

from tqdm import tqdm


def random_string(length: int, chars: str) -> str:
    line = ''.join(random.choice(chars) for _ in range(length))
    return line


def write_lines_to_file(
    file: IO[str],
    num_lines: int,
    max_length: int,
    chars: Optional[str] = None,
    n_jobs: Optional[int] = None,
):
    if not chars:
        chars = string.printable

    for char in string.whitespace:
        if char in ' \t':
            continue
        chars = chars.replace(char, '')

    random_string_closure = partial(random_string, chars=chars)
    line_lens = map(lambda _: random.randint(1, max_length), range(num_lines))

    with mp.Pool(n_jobs) as pool:
        mapping = pool.imap_unordered(random_string_closure, line_lens)
        for line in tqdm(mapping, total=num_lines):
            file.write(line + '\n')


def parse_args():
    parser = ArgumentParser()
    parser.add_argument(
        'path', type=str,
        help='destination file path to write',
    )
    parser.add_argument(
        '--num_lines', type=int, required=True,
        help='number of lines in generated file',
    )
    parser.add_argument(
        '--max_length', type=int, required=True,
        help='maximal length of line',
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
        '--n_jobs', type=int, default=None,
        help='number of CPUs used, omit this one to use all CPU cores',
    )
    return parser.parse_args()


def main():
    args = parse_args()
    chars = getattr(string, args.charset) if args.charset else args.charset
    with open(args.path, mode='w') as file:
        write_lines_to_file(
            file,
            args.num_lines,
            args.max_length,
            chars,
            n_jobs=args.n_jobs,
        )


if __name__ == '__main__':
    main()
