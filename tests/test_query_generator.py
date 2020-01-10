from selection.query_generator import QueryGenerator
from selection.dbms.postgres_dbms import PostgresDatabaseConnector
import unittest


class TestQueryGenerator(unittest.TestCase):
    def setUp(self):
        db_name = 'indexselection_tpch___0_001'
        self.db_connector = PostgresDatabaseConnector(db_name,
                                                      autocommit=True)

    def test_generate_tpch(self):
        query_generator = QueryGenerator('tpch', 0.001,
                                         self.db_connector, None)
        queries = query_generator.queries
        self.assertEqual(len(queries), 22)

    def test_generate_tpcds(self):
        db_name = 'indexselection_tpcds___1'
        db_conn = PostgresDatabaseConnector(db_name, autocommit=True)
        query_generator = QueryGenerator('tpcds', 1,
                                         db_conn, None)
        queries = query_generator.queries
        self.assertEqual(len(queries), 99)

    def test_wrong_benchmark(self):
        with self.assertRaises(NotImplementedError):
            QueryGenerator('tpc-hallo', 1, self.db_connector, None)


if __name__ == '__main__':
    unittest.main()
