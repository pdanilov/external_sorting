from tempfile import NamedTemporaryFile
import unittest

from generate import write_lines_to_file
from sort import merge_sort


class TestGenerate(unittest.TestCase):
    @staticmethod
    def _test(*, num_lines: int, max_length: int):
        in_file = NamedTemporaryFile(mode='r+b')
        write_lines_to_file(in_file, num_lines, max_length)
        in_file.seek(0)

        _num_lines = 0
        for line in in_file:
            assert 0 < len(line) <= max_length+1
            _num_lines += 1

        assert _num_lines == num_lines

    def test_tiny(self):
        self._test(num_lines=5, max_length=5)

    def test_medium(self):
        self._test(num_lines=1_000, max_length=1_000)


class TestSort(unittest.TestCase):
    @staticmethod
    def _test(*, num_lines: int, max_length: int):
        in_file = NamedTemporaryFile(mode='r+b')
        out_file = NamedTemporaryFile(mode='w+b')

        write_lines_to_file(in_file, num_lines, max_length)
        merge_sort(in_file, out_file, chunk_size=1_000)

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
