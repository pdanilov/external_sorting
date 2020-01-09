from tempfile import TemporaryFile
from typing import TextIO
import unittest

from generate import write_lines_to_file
from sort import external_merge_sort
from utils import read_lines_lazy


class TestSorting(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.available_memory = 4 * (1 << 30)  # 4 gigabytes

    @staticmethod
    def assert_sorted(file: TextIO, num_lines: int):
        prev = ''
        idx = 0
        for idx, line in enumerate(read_lines_lazy(file)):
            assert prev <= line, "Sorted condition failed: {} > {}".format(prev, line)
            prev = line
        assert idx + 1 == num_lines, ("Resulting file has wrong number of lines: expected {}, got {}"
                                      .format(num_lines, idx + 1))

    def _test(self, num_lines: int, max_length: int):
        with TemporaryFile(mode='w+') as file:
            write_lines_to_file(file, num_lines, max_length)
            with TemporaryFile(mode='w+') as dst_file:
                external_merge_sort(file, dst_file, max_length, self.available_memory)
                self.assert_sorted(dst_file, num_lines)

    def test_small(self):
        self._test(num_lines=100, max_length=10)

    def test_medium(self):
        self._test(num_lines=1_000, max_length=1_000)

    def test_large(self):
        self._test(num_lines=100_000, max_length=1_000)


if __name__ == '__main__':
    unittest.main()
