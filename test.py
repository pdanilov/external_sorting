from tempfile import TemporaryFile
import unittest

from generate import write_lines_to_file
from sort import merge_sort


class TestSorting(unittest.TestCase):
    @staticmethod
    def _test(*, num_lines: int, max_length: int):
        in_file = TemporaryFile(mode='r+')
        out_file = TemporaryFile(mode='w+')

        write_lines_to_file(in_file, num_lines, max_length)
        merge_sort(in_file, out_file)

        out_file.seek(0)
        src = out_file.readlines()
        in_file.seek(0)
        dst = sorted(in_file.readlines())

        try:
            assert src == dst
        finally:
            in_file.close()
            out_file.close()

    def test_tiny(self):
        self._test(num_lines=10, max_length=10)

    def test_small(self):
        self._test(num_lines=100, max_length=100)

    def test_medium(self):
        self._test(num_lines=1_000, max_length=1_000)

    def test_large(self):
        self._test(num_lines=100_000, max_length=1_000)


if __name__ == '__main__':
    unittest.main()
