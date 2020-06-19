from selection.utils import b_to_mb, mb_to_b
from selection.utils import s_to_ms

import unittest


class TestTableGenerator(unittest.TestCase):
    def test_b_to_mb(self):
        byte = 17000000
        expected_megabyte = 17

        self.assertEqual(b_to_mb(byte), expected_megabyte)

    def test_mb_to_b(self):
        megabyte = 5
        expected_byte = 5000000

        self.assertEqual(mb_to_b(megabyte), expected_byte)

    def test_s_to_ms(self):
        seconds = 17
        expected_milliseconds = 17000

        self.assertEqual(s_to_ms(seconds), expected_milliseconds)
