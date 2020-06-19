from selection.utils import mb_to_b

import unittest


class TestTableGenerator(unittest.TestCase):
    def test_mb_to_b(self):
        megabyte = 5
        expected_byte = 5000000

        self.assertEqual(mb_to_b(megabyte), expected_byte)
