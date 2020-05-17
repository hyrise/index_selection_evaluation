from selection.algorithms.microsoft_algorithm import MicrosoftAlgorithm
from selection.workload import Workload
from selection.index import Index
from tests.mock_connector import (
    MockConnector,
    mock_cache,
    column_A_0,
    column_A_1,
    column_A_2,
    query_0,
    query_1,
)
import unittest


class TestMicrosoftAlgorithm(unittest.TestCase):
    def setUp(self):
        self.connector = MockConnector()
        self.database_name = "test_DB"

    def test_calculate_indexes_2indexes_3columns(self):
        algorithm = MicrosoftAlgorithm(
            database_connector=self.connector,
            parameters={"max_indexes": 2, "max_index_columns": 3},
        )
        algorithm.cost_evaluation.cache = mock_cache
        algorithm.cost_evaluation._prepare_cost_calculation = (
            lambda indexes, store_size=False: None
        )

        index_selection = algorithm.calculate_best_indexes(
            Workload([query_0, query_1], self.database_name)
        )
        self.assertEqual(
            set(index_selection), set([Index([column_A_0, column_A_1, column_A_2])])
        )

    def test_calculate_indexes_2indexes_2columns(self):
        algorithm = MicrosoftAlgorithm(
            database_connector=self.connector,
            parameters={"max_indexes": 2, "max_index_columns": 2},
        )
        algorithm.cost_evaluation.cache = mock_cache
        algorithm.cost_evaluation._prepare_cost_calculation = (
            lambda indexes, store_size=False: None
        )

        index_selection = algorithm.calculate_best_indexes(
            Workload([query_0, query_1], self.database_name)
        )
        self.assertEqual(set(index_selection), set([Index([column_A_0, column_A_1])]))


if __name__ == "__main__":
    unittest.main()
