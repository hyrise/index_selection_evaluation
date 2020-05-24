from selection.algorithms.relaxation_algorithm import RelaxationAlgorithm
from selection.workload import Workload
from selection.index import Index
from tests.mock_connector import (
    MockConnector,
    mock_cache,
    column_A_0,
    column_A_1,
    query_0,
    query_1,
)
import unittest


class TestRelaxationAlgorithm(unittest.TestCase):
    def setUp(self):
        self.connector = MockConnector()
        self.database_name = "test_DB"

    def test_possible_indexes(self):
        algorithm = RelaxationAlgorithm(
            database_connector=self.connector,
            parameters={"max_index_columns": 2, "budget": 1000},
        )
        result = algorithm._possible_indexes(query_0)
        self.assertEqual(len(result), 1)

        result = algorithm._possible_indexes(query_1)
        self.assertEqual(len(result), 9)

        algorithm = RelaxationAlgorithm(
            database_connector=self.connector,
            parameters={"max_index_columns": 3, "budget": 1000},
        )
        result = algorithm._possible_indexes(query_1)
        self.assertEqual(len(result), 15)

        # test with no parameter
        algorithm = RelaxationAlgorithm(
            database_connector=self.connector, parameters={"budget": 1000},
        )
        result = algorithm._possible_indexes(query_1)
        self.assertEqual(len(result), 15)

    @staticmethod
    def set_estimated_index_sizes(indexes, store_size=True):
        for index in indexes:
            if index.estimated_size is None:
                index.estimated_size = len(index.columns) * 1000 * 1000

    @staticmethod
    def set_estimated_index_size(index):
        index.estimated_size = len(index.columns) * 1000 * 1000

    def test_calculate_indexes_3000MB_2column(self):
        algorithm = RelaxationAlgorithm(
            database_connector=self.connector,
            parameters={"max_index_columns": 2, "budget": 3},
        )
        algorithm.cost_evaluation.cache = mock_cache
        algorithm.cost_evaluation._prepare_cost_calculation = (
            self.set_estimated_index_sizes
        )
        algorithm.cost_evaluation.estimate_size = self.set_estimated_index_size
        algorithm._exploit_virtual_indexes = lambda workload: (
            None,
            {
                Index([column_A_0], 1000 * 1000),
                Index([column_A_0, column_A_1], 2000 * 1000),
            },
        )

        index_selection = algorithm.calculate_best_indexes(
            Workload([query_0, query_1], self.database_name)
        )
        self.assertEqual(
            set(index_selection),
            set([Index([column_A_0]), Index([column_A_0, column_A_1])]),
        )

    def test_calculate_indexes_2MB_2column(self):
        algorithm = RelaxationAlgorithm(
            database_connector=self.connector,
            parameters={"max_index_columns": 2, "budget": 2},
        )
        algorithm.cost_evaluation.cache = mock_cache
        algorithm.cost_evaluation._prepare_cost_calculation = (
            self.set_estimated_index_sizes
        )
        algorithm.cost_evaluation.estimate_size = self.set_estimated_index_size
        algorithm._exploit_virtual_indexes = lambda workload: (
            None,
            {
                Index([column_A_0], 1000 * 1000),
                Index([column_A_0, column_A_1], 2000 * 1000),
            },
        )

        index_selection = algorithm.calculate_best_indexes(
            Workload([query_0, query_1], self.database_name)
        )
        self.assertEqual(set(index_selection), set([Index([column_A_0, column_A_1])]))

    def test_calculate_indexes_1MB_2column(self):
        algorithm = RelaxationAlgorithm(
            database_connector=self.connector,
            parameters={"max_index_columns": 2, "budget": 1},
        )
        algorithm.cost_evaluation.cache = mock_cache

        algorithm.cost_evaluation._prepare_cost_calculation = (
            self.set_estimated_index_sizes
        )
        algorithm.cost_evaluation.estimate_size = self.set_estimated_index_size
        algorithm._exploit_virtual_indexes = lambda workload: (
            None,
            {
                Index([column_A_0], 1000 * 1000),
                Index([column_A_0, column_A_1], 2000 * 1000),
            },
        )

        index_selection = algorithm.calculate_best_indexes(
            Workload([query_0, query_1], self.database_name)
        )
        # The single column index is dropped first, because of the lower penalty.
        # The multi column index is prefixed second.
        self.assertEqual(set(index_selection), {Index([column_A_0])})

    def test_calculate_indexes_500kB_2column(self):
        algorithm = RelaxationAlgorithm(
            database_connector=self.connector,
            parameters={"max_index_columns": 2, "budget": 0.5},
        )
        algorithm.cost_evaluation.cache = mock_cache
        algorithm.cost_evaluation._prepare_cost_calculation = (
            self.set_estimated_index_sizes
        )
        algorithm.cost_evaluation.estimate_size = self.set_estimated_index_size
        algorithm._exploit_virtual_indexes = lambda workload: (
            None,
            {
                Index([column_A_0], 1000 * 1000),
                Index([column_A_0, column_A_1], 2000 * 1000),
            },
        )

        index_selection = algorithm.calculate_best_indexes(
            Workload([query_0, query_1], self.database_name)
        )
        self.assertEqual(set(index_selection), set())


if __name__ == "__main__":
    unittest.main()
