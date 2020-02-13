from selection.cost_evaluation import CostEvaluation
from selection.index import Index
from selection.workload import Column, Query, Table, Workload
import unittest
from unittest.mock import MagicMock


class TestIndex(unittest.TestCase):
    @classmethod
    def setUp(self):
        self.column_0 = Column("Col0")
        self.column_1 = Column("Col1")
        self.column_2 = Column("Col2")

        self.columns = [self.column_0, self.column_1]
        self.table = Table('TableA')
        self.table.add_columns(self.columns)
        self.table.add_column(self.column_2)

    def test_index(self):
        index = Index(self.columns)
        self.assertEqual(index.columns, tuple(self.columns))
        self.assertEqual(index.estimated_size, None)
        self.assertEqual(index.hypopg_name, None)

        with self.assertRaises(ValueError):
            index = Index([])

    def test_repr(self):
        index = Index(self.columns)
        self.assertEqual(repr(index), 'I(C tablea.col0,C tablea.col1)')

    def test_index_lt(self):
        index_0 = Index([self.column_0])
        index_1 = Index([self.column_1])

        self.assertTrue(index_0 < index_1)
        self.assertFalse(index_1 < index_0)

        index_0_1_2 = Index(self.columns)
        self.assertTrue(index_0 < index_0_1_2)
        self.assertFalse(index_0_1_2 < index_0)

        index_0_2 = Index([self.column_0, self.column_2])
        self.assertTrue(index_0_1_2 < index_0_2)
        self.assertFalse(index_0_2 < index_0_1_2)

    def test_index_eq(self):
        index_0 = Index([self.column_0])
        index_1 = Index([self.column_1])
        index_2 = Index([self.column_0])

        self.assertFalse(index_0 == index_1)
        self.assertTrue(index_0 == index_2)

        index_0_1_2 = Index(self.columns)
        index_0_1 = Index([self.column_0, self.column_1])
        self.assertTrue(index_0_1_2 == index_0_1)

        # Check comparing object of different class
        self.assertFalse(index_0_1_2 == int(3))

    def test_index_column_names(self):
        index = Index(self.columns)
        column_names = index._column_names()
        self.assertEqual(column_names, ['col0', 'col1'])

    def test_index_is_single_column(self):
        index_2 = Index([self.column_2])
        index_0_1_2 = Index(self.columns)

        self.assertTrue(index_2.is_single_column())
        self.assertFalse(index_0_1_2.is_single_column())

    def test_index_table(self):
        index = Index(self.columns)

        table = index.table()
        self.assertEqual(table, self.table)

    def test_index_idx(self):
        index = Index(self.columns)

        index_idx = index.index_idx()
        self.assertEqual(index_idx, 'tablea_col0_col1_idx')

    def test_joined_column_names(self):
        index = Index(self.columns)

        index_idx = index.joined_column_names()
        self.assertEqual(index_idx, 'col0,col1')

    def test_appendable_by_other_table(self):
        column = Column('ColZ')
        table = Table('TableZ')
        table.add_column(column)
        index_on_other_table = Index([column])

        index = Index([self.column_0])

        self.assertFalse(index.appendable_by(index_on_other_table))

    def test_appendable_by_multi_column_index(self):
        multi_column_index = Index(self.columns)

        index = Index([self.column_2])

        self.assertFalse(index.appendable_by(multi_column_index))

    def test_appendable_by_index_with_already_present_column(self):
        index_with_already_present_column = Index([self.column_0])

        index = Index(self.columns)

        self.assertFalse(
            index.appendable_by(index_with_already_present_column))

    def test_appendable_by(self):
        index_appendable_by = Index([self.column_2])

        index = Index(self.columns)

        self.assertTrue(index.appendable_by(index_appendable_by))

    def test_appendable_by_other_type(self):
        index = Index(self.columns)

        self.assertFalse(index.appendable_by(int(17)))

    def test_subsumes(self):
        index_0 = Index([self.column_0])
        index_0_other = Index([self.column_0])
        index_1 = Index([self.column_1])
        index_0_1 = Index([self.column_0, self.column_0])
        index_0_2 = Index([self.column_0, self.column_2])
        index_1_0 = Index([self.column_1, self.column_0])

        self.assertTrue(index_0.subsumes(index_0_other))
        
        self.assertFalse(index_0.subsumes(index_1))
        
        self.assertTrue(index_0_1.subsumes(index_0))
        self.assertFalse(index_0_1.subsumes(index_1))
        self.assertFalse(index_0_1.subsumes(index_0_2))
        self.assertFalse(index_0_1.subsumes(index_1_0))

        self.assertFalse(index_0.subsumes(index_0_1))

        self.assertFalse(index_0.subsumes(int(17)))


if __name__ == '__main__':
    unittest.main()
