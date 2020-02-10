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
    def setUpClass(cls):
        cls.db_name = "TestDB"

        cls.table = Table("TestTableA")
        cls.columns = [
            Column("Col0"),
            Column("Col1"),
            Column("Col2"),
            Column("Col3"),
            Column("Col4")
        ]
        cls.table.add_columns(cls.columns)

        cls.queries = [
            Query(0, "SELECT * FROM TestTableA WHERE Col0 = 4", [cls.columns[0]]),
            Query(1, "SELECT * FROM TestTableA WHERE Col1 = 3", [cls.columns[1]]),
            Query(2, "SELECT * FROM TestTableA WHERE Col0 = 14 AND Col1 = 13", [cls.columns[0], cls.columns[1]]),
        ]

        cls.workload = Workload(cls.queries, cls.db_name)

    def setUp(self):
        # We mock the connector because it is needed by the cost evaluation.
        # By also mocking some of its methods, we can test how often these are called.
        self.connector = MockConnector()
        self.connector.get_cost = MagicMock(return_value=3)
        self.connector.simulate_index = MagicMock(return_value=[0, 'index_name']) #index_oid, index_name
        
        self.cost_evaluation = CostEvaluation(self.connector)

    def tearDown(self):
        self.connector.simulate_index.reset_mock()
        self.connector.get_cost.reset_mock()

    def test_relevant_indexes(self):
        index_A = Index([self.columns[0]])
        index_B = Index([self.columns[1]])

        result = self.cost_evaluation._relevant_indexes(self.queries[0], indexes=set())
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
            self.cost_evaluation.calculate_cost(self.workload, indexes=set())

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

        self.cost_evaluation.calculate_cost(workload, indexes=set())
        self.assertEqual(self.cost_evaluation.cost_requests, 1)
        self.assertEqual(self.cost_evaluation.cache_hits, 0)
        self.assertEqual(self.connector.get_cost.call_count, 1)

        self.cost_evaluation.calculate_cost(workload, indexes=set())
        self.assertEqual(self.cost_evaluation.cost_requests, 2)
        self.assertEqual(self.cost_evaluation.cache_hits, 1)
        self.assertEqual(self.connector.get_cost.call_count, 1)

    def test_no_cache_hit_unseen(self):
        self.assertEqual(self.cost_evaluation.cost_requests, 0)
        self.assertEqual(self.cost_evaluation.cache_hits, 0)

        workload = Workload([self.queries[0]], self.db_name)
        index_A = Index([self.columns[0]])

        self.cost_evaluation.calculate_cost(workload, indexes=set())
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

        self.cost_evaluation.calculate_cost(workload, indexes=set())
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
