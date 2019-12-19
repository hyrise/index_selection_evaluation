from selection.selection_algorithm import SelectionAlgorithm
from selection.dbms.postgres_dbms import PostgresDatabaseConnector
from selection.table_generator import TableGenerator
from selection.workload import Workload

import unittest


class TestAlgorithm(unittest.TestCase):
    def setUp(self):
        self.db_connector = PostgresDatabaseConnector(None,
                                                      autocommit=True)
        tab_gen = TableGenerator('tpch', 0.001, self.db_connector)
        self.db_name = tab_gen.database_name()
        self.selection_algorithm = SelectionAlgorithm(self.db_connector,
                                                      {'test': 24})

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

    def test_cost_eval_cost(self):
        workload = Workload([], self.db_name)
        cost_eval = self.selection_algorithm.cost_evaluation
        cost = cost_eval.calculate_cost(workload, [])
        self.assertEqual(cost, 0)


if __name__ == '__main__':
    unittest.main()
