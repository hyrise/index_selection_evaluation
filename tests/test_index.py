from selection.cost_evaluation import CostEvaluation
from selection.index import Index
from selection.workload import Column, Query, Table, Workload
import unittest
from unittest.mock import MagicMock


class TestIndex(unittest.TestCase):
    @classmethod
    def setUp(self):
        self.column_1 = Column("ColA")
        self.column_2 = Column("ColB")
        self.column_3 = Column("ColC")

        self.columns = [self.column_1, self.column_2]
        self.table = Table('TableA')
        self.table.add_columns(self.columns)
        self.table.add_column(self.column_3)


    def test_index(self):
        index = Index(self.columns)
        self.assertEqual(index.columns, tuple(self.columns))
        self.assertEqual(index.estimated_size, None)
        self.assertEqual(index.hypopg_name, None)

        with self.assertRaises(ValueError):
            index = Index([])

    def test_repr(self):
        index = Index(self.columns)
        self.assertEqual(repr(index), 'I(C tablea.cola,C tablea.colb)')

    def test_index_lt(self):
        index_1 = Index([self.column_1])
        index_2 = Index([self.column_2])

        self.assertTrue(index_1 < index_2)
        self.assertFalse(index_2 < index_1)

        index_3 = Index(self.columns)
        self.assertTrue(index_1 < index_3)
        self.assertFalse(index_3 < index_1)

        index_4 = Index([self.column_1, self.column_3])
        self.assertTrue(index_3 < index_4)
        self.assertFalse(index_4 < index_3)

    def test_index_eq(self):
        index_1 = Index([self.column_1])
        index_2 = Index([self.column_2])
        index_3 = Index([self.column_1])

        self.assertFalse(index_1 == index_2)
        self.assertTrue(index_1 == index_3)

        index_4 = Index(self.columns)
        index_5 = Index([self.column_1, self.column_2])
        self.assertTrue(index_4 == index_5)

        # Check comparing object of different class
        self.assertFalse(index_4 == int(3))

    def test_index_column_names(self):
        index = Index(self.columns)
        column_names = index._column_names()
        self.assertEqual(column_names, ['cola', 'colb'])

    def test_index_is_single_column(self):
        index_1 = Index([self.column_3])
        index_2 = Index(self.columns)

        self.assertTrue(index_1.is_single_column())
        self.assertFalse(index_2.is_single_column())

    def test_index_table(self):
        index = Index(self.columns)

        table = index.table()
        self.assertEqual(table, self.table)

    def test_index_idx(self):
        index = Index(self.columns)

        index_idx = index.index_idx()
        self.assertEqual(index_idx, 'tablea_cola_colb_idx')

    def test_joined_column_names(self):
        index = Index(self.columns)

        index_idx = index.joined_column_names()
        self.assertEqual(index_idx, 'cola,colb')

    def test_appendable_by_other_table(self):
        column = Column('ColZ')
        table = Table('TableZ')
        table.add_column(column)
        index_on_other_table = Index([column])

        index = Index([self.column_1])

        self.assertFalse(index.appendable_by(index_on_other_table))

    def test_appendable_by_multi_column_index(self):
        multi_column_index = Index(self.columns)

        index = Index([self.column_3])

        self.assertFalse(index.appendable_by(multi_column_index))

    def test_appendable_by_index_with_already_present_column(self):
        index_with_already_present_column = Index([self.column_1])

        index = Index(self.columns)

        self.assertFalse(index.appendable_by(index_with_already_present_column))

    def test_appendable_by(self):
        index_appendable_by = Index([self.column_3])

        index = Index(self.columns)

        self.assertTrue(index.appendable_by(index_appendable_by))


if __name__ == '__main__':
    unittest.main()
