from selection.selection_algorithm import SelectionAlgorithm
from selection.dbms.postgres_dbms import PostgresDatabaseConnector
from selection.table_generator import TableGenerator
from selection.workload import Workload

import unittest


class TestAlgorithm(unittest.TestCase):
    @classmethod
    def setUpClass(self):
        self.db_name = 'tpch_test_db_algorithm'

        self.db_connector = PostgresDatabaseConnector(None,
                                                      autocommit=True)
        tab_gen = TableGenerator('tpch', 0.001, self.db_connector, explicit_database_name=self.db_name)
        self.selection_algorithm = SelectionAlgorithm(self.db_connector,
                                                      {'test': 24})

        self.db_connector.close()

    @classmethod
    def tearDownClass(self):
        connector = PostgresDatabaseConnector(None,
                                                      autocommit=True)
        if connector.database_exists(self.db_name):
            connector.drop_database(self.db_name)

    def test_parameters(self):
        params = self.selection_algorithm.parameters
        self.assertEqual(params, {'test': 24})

    def test_calculate_best(self):
        workload = Workload([], self.db_name)
        with self.assertRaises(NotImplementedError):
            self.selection_algorithm.calculate_best_indexes(workload)

    def test_cost_eval(self):
        db_conn = self.selection_algorithm.cost_evaluation.db_connector
        self.assertEqual(db_conn, self.db_connector)

    def test_cost_eval_cost_empty_workload(self):
        workload = Workload([], self.db_name)
        cost_eval = self.selection_algorithm.cost_evaluation
        cost = cost_eval.calculate_cost(workload, [])
        self.assertEqual(cost, 0)


if __name__ == '__main__':
    unittest.main()
