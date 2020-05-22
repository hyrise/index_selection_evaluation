from selection.algorithms.relaxation_algorithm import RelaxationAlgorithm
from selection.workload import Workload
from selection.index import Index
from tests.mock_connector import (
    MockConnector,
    mock_cache,
    column_A_0,
    query_0,
    query_1,
)
import unittest


class TestRelaxationAlgorithm(unittest.TestCase):
    def setUp(self):
        self.connector = MockConnector()
        self.database_name = "test_DB"

    def test_calculate_indexes_1000MB_1column(self):
        algorithm = RelaxationAlgorithm(
            database_connector=self.connector,
            parameters={"max_indexes": 2, "budget": 1000},
        )
        algorithm.cost_evaluation.cache = mock_cache
        algorithm.cost_evaluation._prepare_cost_calculation = (
            lambda indexes, store_size=True: None
        )
        algorithm._exploit_virtual_indexes = lambda workload: (
            None,
            set([Index([column_A_0], 100)]),
        )

        index_selection = algorithm.calculate_best_indexes(
            Workload([query_0, query_1], self.database_name)
        )
        self.assertEqual(set(index_selection), set([Index([column_A_0])]))


if __name__ == "__main__":
    unittest.main()
