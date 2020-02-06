from selection.cost_evaluation import CostEvaluation
from selection.index import Index
from selection.workload import Column, Query, Table, Workload
import unittest
from unittest.mock import MagicMock

class MockConnector:
    def __init__(self):
        pass

class MockWhatIf:
    def __init__(self):
        pass


class TestCostEvaluation(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.db_name = "TestDB"

        cls.table = Table("TestTableA")
        cls.columns = [
            Column("ColA", cls.table),
            Column("ColB", cls.table),
            Column("ColC", cls.table),
            Column("ColD", cls.table),
            Column("ColE", cls.table)
        ]

        cls.queries = [
            Query(0, "SELECT * FROM TestTableA WHERE ColA = 4", [cls.columns[0]]),
            Query(1, "SELECT * FROM TestTableA WHERE ColB = 3", [cls.columns[1]]),
            Query(2, "SELECT * FROM TestTableA WHERE Col A = 14 AND ColB = 13", [cls.columns[0], cls.columns[1]]),
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

    def test_cache_hit_different_index_same_columns(self):
        self.assertEqual(self.cost_evaluation.cost_requests, 0)
        self.assertEqual(self.cost_evaluation.cache_hits, 0)

        workload = Workload([self.queries[0]], self.db_name)
        index_A = Index([self.columns[0]])
        index_B = Index([self.columns[0]])

        self.cost_evaluation.calculate_cost(workload, set([index_A]))
        self.assertEqual(self.cost_evaluation.cost_requests, 1)
        self.assertEqual(self.cost_evaluation.cache_hits, 0)
        self.assertEqual(self.connector.get_cost.call_count, 1)

        self.cost_evaluation.calculate_cost(workload, set([index_B]))
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

class TestPrepareCostEvaluation(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.db_name = "TestDB"

        cls.table = Table("TestTableA")
        cls.columns = [
            Column("ColA", cls.table),
            Column("ColB", cls.table),
            Column("ColC", cls.table),
            Column("ColD", cls.table),
            Column("ColE", cls.table)
        ]

    def setUp(self):
        self.mock_what_if = MockWhatIf()
        self.mock_what_if.simulate_index = MagicMock()
        self.mock_what_if.drop_simulated_index = MagicMock()

        self.cost_evaluation = CostEvaluation(MockConnector())
        self.cost_evaluation.what_if = self.mock_what_if

    def test_prepare_cost_calculation_does_nothing_empty_indexes(self):
        self.cost_evaluation.current_indexes = set([])

        self.cost_evaluation._prepare_cost_calculation(set([]))
        self.mock_what_if.simulate_index.assert_not_called()
        self.mock_what_if.drop_simulated_index.assert_not_called()
        self.assertEqual(self.cost_evaluation.current_indexes, set([]))

    def test_prepare_cost_calculation_does_nothing_indexes_equal_current_indexes(self):
        index_A = Index([self.columns[0]])
        index_B = Index([self.columns[1]])
        self.cost_evaluation.current_indexes = set([index_A, index_B])

        self.cost_evaluation._prepare_cost_calculation([index_A, index_B])
        self.mock_what_if.simulate_index.assert_not_called()
        self.mock_what_if.drop_simulated_index.assert_not_called()
        self.assertEqual(self.cost_evaluation.current_indexes, set([index_A, index_B]))

    def test_prepare_cost_calculation_index_removed(self):
        index_A = Index([self.columns[0]])
        index_B = Index([self.columns[1]])
        self.cost_evaluation.current_indexes = set([index_A, index_B])

        self.cost_evaluation._prepare_cost_calculation([index_A])
        self.mock_what_if.simulate_index.assert_not_called()
        self.mock_what_if.drop_simulated_index.assert_called_with(index_B)
        self.assertEqual(self.cost_evaluation.current_indexes, set([index_A]))

    def test_prepare_cost_calculation_index_added(self):
        index_A = Index([self.columns[0]])
        index_B = Index([self.columns[1]])
        self.cost_evaluation.current_indexes = set([index_A])

        self.cost_evaluation._prepare_cost_calculation([index_A, index_B])
        self.mock_what_if.simulate_index.assert_called_with(index_B, store_size=False)
        self.mock_what_if.drop_simulated_index.assert_not_called()
        self.assertEqual(self.cost_evaluation.current_indexes, set([index_A, index_B]))

    def test_prepare_cost_calculation_index_added_and_removed(self):
        index_A = Index([self.columns[0]])
        index_B = Index([self.columns[1]])
        index_C = Index([self.columns[2]])
        self.cost_evaluation.current_indexes = set([index_A, index_B])

        self.cost_evaluation._prepare_cost_calculation([index_A, index_C])
        self.mock_what_if.simulate_index.assert_called_with(index_C, store_size=False)
        self.mock_what_if.drop_simulated_index.assert_called_with(index_B)
        self.assertEqual(self.cost_evaluation.current_indexes, set([index_A, index_C]))

    def test_complete_cost_estimation(self):
        index_A = Index([self.columns[0]])
        index_B = Index([self.columns[1]])
        self.cost_evaluation.current_indexes = set([index_A, index_B])

        self.cost_evaluation.complete_cost_estimation()
        self.mock_what_if.drop_simulated_index.assert_any_call(index_A)
        self.mock_what_if.drop_simulated_index.assert_any_call(index_B)
        self.assertEqual(self.cost_evaluation.current_indexes, set())

    def test_reset(self):
        index_A = Index([self.columns[0]])
        self.cost_evaluation.current_indexes = set([index_A])

        self.cost_evaluation.reset()
        self.mock_what_if.drop_simulated_index.assert_called_with(index_A)
        self.assertEqual(self.cost_evaluation.current_indexes, set())
        self.assertEqual(self.cost_evaluation.cost_requests, 0)
        self.assertEqual(self.cost_evaluation.cache_hits, 0)
        self.assertEqual(self.cost_evaluation.cache, {})


if __name__ == '__main__':
    unittest.main()
