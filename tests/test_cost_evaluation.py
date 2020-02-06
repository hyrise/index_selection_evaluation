from selection.cost_evaluation import CostEvaluation
from selection.index import Index
from selection.workload import Column, Query, Table, Workload
import unittest
from unittest.mock import MagicMock

class MockConnector:
    def __init__(self):
        pass


class TestCostEvaluation(unittest.TestCase):
    @classmethod
    def setUpClass(self):
        self.db_name = "TestDB"

        self.table = Table("TestTableA")
        self.columns = [
            Column("ColA", self.table),
            Column("ColB", self.table),
            Column("ColC", self.table),
            Column("ColD", self.table),
            Column("ColE", self.table)
        ]

        self.queries = [
            Query(1, "SELECT * FROM TestTableA WHERE ColA = 4", [self.columns[0]]),
            Query(2, "SELECT * FROM TestTableA WHERE ColB = 3", [self.columns[1]]),
            Query(3, "SELECT * FROM TestTableA WHERE Col A = 14 AND ColB = 13", [self.columns[0], self.columns[1]]),
        ]

        self.workload = Workload(self.queries, self.db_name)

        self.no_indexes = set()

    def setUp(self):
        self.connector = MockConnector()
        self.connector.get_cost = MagicMock(return_value=3)
        self.connector.simulate_index = MagicMock(return_value=[0, 1])
        
        self.cost_evaluation = CostEvaluation(self.connector)

    def tearDown(self):
        self.connector.simulate_index.reset_mock()
        self.connector.get_cost.reset_mock()

    def test_relevant_indexes(self):
        index_A = Index([self.columns[0]])
        index_B = Index([self.columns[1]])

        result = self.cost_evaluation._relevant_indexes(self.queries[0], self.no_indexes)
        self.assertEqual(result, frozenset())

        result = self.cost_evaluation._relevant_indexes(self.queries[0], set([index_A]))
        self.assertEqual(result, frozenset([index_A]))

        result = self.cost_evaluation._relevant_indexes(self.queries[0], set([index_B, index_A]))
        self.assertEqual(result, frozenset([index_A]))

        result = self.cost_evaluation._relevant_indexes(self.queries[2], set([index_B, index_A]))
        self.assertEqual(result, frozenset([index_B, index_A]))


    def test_cost_requests(self):
        self.assertEqual(self.cost_evaluation.cost_requests, 0)

        CALCULATE_COST_CALLS = 3
        for i in range(CALCULATE_COST_CALLS):
            self.cost_evaluation.calculate_cost(self.workload, self.no_indexes)

        expected_cost_requests = len(self.workload.queries) * CALCULATE_COST_CALLS
        self.assertEqual(self.cost_evaluation.cost_requests, expected_cost_requests)

        # Since we did not change the index configuration, all calls, except the first round, should be cached
        expected_cache_hits = expected_cost_requests - len(self.workload.queries)
        self.assertEqual(self.cost_evaluation.cache_hits, expected_cache_hits)

        # Therefore, actual calls to the database connector's get_cost method should be limited by the number
        # of queries as it is not called for cached costs.
        self.assertEqual(self.connector.get_cost.call_count, len(self.workload.queries))

    def test_cache_hit(self):
        self.assertEqual(self.cost_evaluation.cost_requests, 0)
        self.assertEqual(self.cost_evaluation.cache_hits, 0)

        workload = Workload([self.queries[0]], self.db_name)

        self.cost_evaluation.calculate_cost(workload, self.no_indexes)
        self.assertEqual(self.cost_evaluation.cost_requests, 1)
        self.assertEqual(self.cost_evaluation.cache_hits, 0)
        self.assertEqual(self.connector.get_cost.call_count, 1)

        self.cost_evaluation.calculate_cost(workload, self.no_indexes)
        self.assertEqual(self.cost_evaluation.cost_requests, 2)
        self.assertEqual(self.cost_evaluation.cache_hits, 1)
        self.assertEqual(self.connector.get_cost.call_count, 1)

    def test_no_cache_hit_unseen(self):
        self.assertEqual(self.cost_evaluation.cost_requests, 0)
        self.assertEqual(self.cost_evaluation.cache_hits, 0)

        workload = Workload([self.queries[0]], self.db_name)
        index_A = Index([self.columns[0]])

        self.cost_evaluation.calculate_cost(workload, self.no_indexes)
        self.assertEqual(self.cost_evaluation.cost_requests, 1)
        self.assertEqual(self.cost_evaluation.cache_hits, 0)
        self.assertEqual(self.connector.get_cost.call_count, 1)

        self.cost_evaluation.calculate_cost(workload, set([index_A]))
        self.assertEqual(self.cost_evaluation.cost_requests, 2)
        self.assertEqual(self.cost_evaluation.cache_hits, 0)
        self.assertEqual(self.connector.get_cost.call_count, 2)
        self.connector.simulate_index.assert_called_with(index_A)


    def test_cache_hit_non_relevant_index(self):
        self.assertEqual(self.cost_evaluation.cost_requests, 0)
        self.assertEqual(self.cost_evaluation.cache_hits, 0)

        workload = Workload([self.queries[0]], self.db_name)
        index_B = Index([self.columns[1]])

        self.cost_evaluation.calculate_cost(workload, self.no_indexes)
        self.assertEqual(self.cost_evaluation.cost_requests, 1)
        self.assertEqual(self.cost_evaluation.cache_hits, 0)
        self.assertEqual(self.connector.get_cost.call_count, 1)

        self.cost_evaluation.calculate_cost(workload, set([index_B]))
        self.assertEqual(self.cost_evaluation.cost_requests, 2)
        self.assertEqual(self.cost_evaluation.cache_hits, 1)
        self.assertEqual(self.connector.get_cost.call_count, 1)
        self.connector.simulate_index.assert_called_with(index_B)


if __name__ == '__main__':
    unittest.main()
