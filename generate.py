from argparse import ArgumentParser
from functools import partial
import multiprocessing as mp
import random
import string
from typing import Optional, TextIO

from tqdm import tqdm

from utils import get_logger_with_console_handler

logger = get_logger_with_console_handler(__name__)


def random_string(length: int, chars: str) -> str:
    line = ''.join([random.choice(chars) for _ in range(length)])
    return line


def write_lines_to_file(
        file: TextIO,
        num_lines: int,
        max_length: int,
        chars: str = string.ascii_letters,
        n_jobs: Optional[int] = None,
):
    random_string_closure = partial(random_string, chars=chars)
    line_lens = map(lambda _: random.randint(1, max_length), range(num_lines))

    with mp.Pool(n_jobs) as pool:
        mapping = pool.imap_unordered(random_string_closure, line_lens)

        logger.info("Generating data.")

        for line in tqdm(mapping, total=num_lines):
            file.write(line + '\n')

    file.seek(0)


def _parse_args():
    parser = ArgumentParser()
    parser.add_argument('--file', type=str, required=True,
                        help='destination file to write')
    parser.add_argument('--num_lines', type=int, required=True,
                        help='number of lines in generated file')
    parser.add_argument('--max_length', type=int, required=True,
                        help='maximal length of line')
    parser.add_argument('--charset', default='letters', choices=['letters', 'digits', 'letters_digits', 'printable'],
                        help='alphabet of characters to use')
    parser.add_argument('--n_jobs', type=int, default=None,
                        help='number of CPUs used, omit this one to use all CPU cores')
    return parser.parse_args()


def _parse_charset(charset: str) -> str:
    if charset == 'letters':
        chars = string.ascii_letters
    elif charset == 'digits':
        chars = string.digits
    elif charset == 'letters_digits':
        chars = string.ascii_letters + string.digits
    elif charset == 'printable':
        # to support Windows its necessary to also drop '\r'
        chars = string.printable.replace('\n', '').replace('\r', '')
    else:
        raise ValueError("Unexpected type of charset: '{}'".format(charset))

    return chars


def _write_lines(path: str, num_lines: int, max_length: int, chars: str, n_jobs: Optional[int] = None):
    with open(path, mode='w') as file:
        write_lines_to_file(file, num_lines, max_length, chars=chars, n_jobs=n_jobs)


def main():
    args = _parse_args()
    chars = _parse_charset(args.charset)
    _write_lines(args.file, args.num_lines, args.max_length, chars, n_jobs=args.n_jobs)


if __name__ == '__main__':
    main()
