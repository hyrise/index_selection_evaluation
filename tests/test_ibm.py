from selection.algorithms.ibm_algorithm import IBMAlgorithm
from selection.index import Index
from selection.workload import Column, Query, Table, Workload

import unittest
from unittest.mock import MagicMock

class MockConnector:
    def __init__(self):
        pass

    def drop_indexes(self):
        pass

# class MockCostEvaluation:
#     def __init__(self):
#         pass

# def index_combination_to_str(index_combination):
#     indexes_as_str = sorted([x.index_idx() for x in index_combination])

#     return '||'.join(indexes_as_str)


MB_TO_BYTES = 1000000


class TestIBMAlgorithm(unittest.TestCase):

    def setUp(self):
        self.connector = MockConnector()
        self.algo = IBMAlgorithm(database_connector=self.connector)

        self.column_0 = Column('Col0')
        self.column_1 = Column('Col1')
        self.column_2 = Column('Col2')
        self.all_columns = [self.column_0, self.column_1, self.column_2]

        self.table = Table('Table0')
        self.table.add_columns(self.all_columns)

        # self.index_1 = Index([self.column_0])
        # self.index_1.estimated_size = 5
        # self.index_2 = Index([self.column_1])
        # self.index_2.estimated_size = 1
        # self.index_3 = Index([self.column_2])
        # self.index_3.estimated_size = 3

        self.query_0 = Query(0, 'SELECT * FROM Table0 WHERE Col0 = 1 AND Col1 = 2 AND Col2 = 3;', self.all_columns)
        # query_1 = Query(1, 'SELECT * FROM TableA WHERE ColA = 4;', [self.column_0])

    def test_ibm_algorithm(self):
        # Should use default parameters if none are specified
        budget_in_mb = 500
        self.assertEqual(self.algo.disk_constraint, budget_in_mb * MB_TO_BYTES)
        self.assertEqual(self.algo.cost_evaluation.cost_estimation, 'whatif')
        self.assertEqual(self.algo.seconds_limit, 10)
        self.assertEqual(self.algo.maximum_remove, 4)

    def test_possible_indexes(self):
        column_0_table_1 = Column('Col0')
        table_1 = Table("Table1")
        table_1.add_column(column_0_table_1)
        query = Query(17, 'SELECT * FROM Table0 as t0, Table1 as t1 WHERE t0.Col0 = 1 AND t0.Col1 = 2 AND t0.Col2 = 3 AND t1.Col0 = 17;', [self.column_0, self.column_1, self.column_2, column_0_table_1])
        indexes = self.algo._possible_indexes(query)
        self.assertIn(Index([column_0_table_1]), indexes)
        self.assertIn(Index([self.column_0]), indexes)
        self.assertIn(Index([self.column_1]), indexes)
        self.assertIn(Index([self.column_2]), indexes)
        self.assertIn(Index([self.column_0, self.column_1]), indexes)
        self.assertIn(Index([self.column_0, self.column_2]), indexes)
        self.assertIn(Index([self.column_1, self.column_0]), indexes)
        self.assertIn(Index([self.column_1, self.column_2]), indexes)
        self.assertIn(Index([self.column_2, self.column_0]), indexes)
        self.assertIn(Index([self.column_2, self.column_1]), indexes)
        self.assertIn(Index([self.column_0, self.column_1, self.column_2]), indexes)
        self.assertIn(Index([self.column_0, self.column_2, self.column_1]), indexes)
        self.assertIn(Index([self.column_1, self.column_0, self.column_2]), indexes)
        self.assertIn(Index([self.column_1, self.column_2, self.column_0]), indexes)
        self.assertIn(Index([self.column_2, self.column_0, self.column_1]), indexes)
        self.assertIn(Index([self.column_2, self.column_1, self.column_0]), indexes)

    def test_recommended_indexes(self):
        def _simulate_index_mock(index, store_size):
            index.hypopg_name = f'<1337>btree_{index.columns}'

        # For some reason, the database decides to only use an index for one of the filters
        def _simulate_get_plan(query):
            plan = {
                'Total Cost': 17,
                'Plans': [
                    {
                        "Index Name": "<1337>btree_(C table0.col1,)",
                        "Filter": "(Col0 = 1)"
                    }
                ]
            }

            return plan

        query = Query(17, 'SELECT * FROM Table0 WHERE Col0 = 1 AND Col1 = 2;', [self.column_0, self.column_1])
        self.algo.database_connector.get_plan = MagicMock(side_effect=_simulate_get_plan)
        self.algo.what_if.simulate_index = MagicMock(side_effect=_simulate_index_mock)
        self.algo.what_if.drop_all_simulated_indexes = MagicMock()

        indexes, cost = self.algo._recommended_indexes(query)
        self.assertEqual(cost, 17)
        self.assertEqual(indexes, [Index([self.column_1])])

        self.assertEqual(self.algo.what_if.simulate_index.call_count, 4)
        self.algo.what_if.drop_all_simulated_indexes.assert_called_once()
        self.algo.database_connector.get_plan.assert_called_once_with(query)


