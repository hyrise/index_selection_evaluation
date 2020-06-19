from selection.index import Index
from selection.utils import b_to_mb, mb_to_b
from selection.utils import s_to_ms
from selection.utils import indexes_by_table
from selection.workload import Column, Table

import unittest


class TestTableGenerator(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.column_a_0 = Column("Col0")
        cls.column_a_1 = Column("Col1")
        cls.table_a = Table("TableA")
        cls.table_a.add_columns([cls.column_a_0, cls.column_a_1])

        cls.column_b_0 = Column("Col0")
        cls.table_b = Table("TableB")
        cls.table_b.add_columns([cls.column_b_0])

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

    def test_indexes_by_table(self):
        empty_index_set = []
        expected = {}
        self.assertEqual(indexes_by_table(empty_index_set), expected)

        index_0 = Index([self.column_a_0])
        index_1 = Index([self.column_b_0])
        index_2 = Index([self.column_a_1])

        index_set = [index_0, index_1, index_2]
        expected = {
            self.table_a: [index_0, index_2],
            self.table_b: [index_1]
        }
        self.assertEqual(indexes_by_table(index_set), expected)