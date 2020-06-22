import sys
import unittest

from selection.candidate_generation import (
    candidates_per_query,
    syntactically_relevant_indexes,
)
from selection.dbms.postgres_dbms import PostgresDatabaseConnector
from selection.index_selection_evaluation import IndexSelection
from selection.query_generator import QueryGenerator
from selection.table_generator import TableGenerator
from selection.workload import Workload


class TestIndexSelection(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.db_name = "tpch_test_db_index_selection"
        cls.index_selection = IndexSelection()
        db = PostgresDatabaseConnector(None, autocommit=True)
        table_gen = TableGenerator("tpch", 0.001, db, explicit_database_name=cls.db_name)
        db.close()

        cls.index_selection.setup_db_connector(cls.db_name, "postgres")

        # Filter worklaod
        query_gen = QueryGenerator(
            "tpch", 0.001, cls.index_selection.db_connector, [3, 14], table_gen.columns
        )
        cls.small_tpch = Workload(query_gen.queries)

    @classmethod
    def tearDownClass(cls):
        cls.index_selection.db_connector.close()

        connector = PostgresDatabaseConnector(None, autocommit=True)

        if connector.database_exists(cls.db_name):
            connector.drop_database(cls.db_name)

    def test_constructor(self):
        ind_sel = IndexSelection()
        ind_sel

    def test_microsoft_algorithm(self):
        parameters = {"max_indexes": 3, "max_indexes_naive": 1}
        algorithm = self.index_selection.create_algorithm_object("microsoft", parameters)
        algorithm.calculate_best_indexes(self.small_tpch)

    def test_all_indexes_algorithm(self):
        algo = self.index_selection.create_algorithm_object("all_indexes", None)
        algo.calculate_best_indexes(self.small_tpch)

    def test_drop_algorithm(self):
        parameters = {"max_indexes": 4}
        algo = self.index_selection.create_algorithm_object("drop_heuristic", parameters)
        indexes = algo.calculate_best_indexes(self.small_tpch)
        self.assertEqual(len(indexes), 4)

    def test_dexter_algorithm(self):
        parameters = {}
        algo = self.index_selection.create_algorithm_object("dexter", parameters)
        indexes = algo.calculate_best_indexes(self.small_tpch)
        self.assertTrue(len(indexes) >= 1)

    def test_ibm_algorithm(self):
        parameters = {}
        algo = self.index_selection.create_algorithm_object("ibm", parameters)
        workload = Workload([self.small_tpch.queries[0]])

        possible = candidates_per_query(
            workload,
            max_index_width=3,
            candidate_generator=syntactically_relevant_indexes,
        )[0]
        indexes = algo.calculate_best_indexes(workload)
        self.assertTrue(len(possible) >= len(indexes))

    def test_run_cli_config(self):
        sys.argv = [sys.argv[0]]
        sys.argv.append("tests/config_tests.json")
        sys.argv.append("ERROR_LOG")
        sys.argv.append("DISABLE_OUTPUT_FILES")
        self.index_selection.run()

    def test_run_cli_config_timeout(self):
        sys.argv = [sys.argv[0]]
        sys.argv.append("tests/config_test_timeout.json")
        sys.argv.append("CRITICAL_LOG")
        sys.argv.append("DISABLE_OUTPUT_FILES")
        self.index_selection.run()


if __name__ == "__main__":
    unittest.main()
