import unittest

from selection.dbms.postgres_dbms import PostgresDatabaseConnector
from selection.query_generator import QueryGenerator
from selection.table_generator import TableGenerator


class TestQueryGenerator(unittest.TestCase):
    def setUp(self):
        self.db_name = None
        self.generating_connector = PostgresDatabaseConnector(None, autocommit=True)

    def tearDown(self):
        self.generating_connector.close()
        connector = PostgresDatabaseConnector(None, autocommit=True)

        if (
            self.db_name is not None
            and self.db_name != "indexselection_job___1"
            and connector.database_exists(self.db_name)
        ):
            connector.drop_database(self.db_name)

    def test_generate_tpch(self):
        self.db_name = "tpch_test_db"

        TableGenerator(
            "tpch", 0.001, self.generating_connector, explicit_database_name=self.db_name,
        )

        db_connector = PostgresDatabaseConnector(self.db_name, autocommit=True)
        query_generator = QueryGenerator("tpch", 0.001, db_connector, None, [])
        queries = query_generator.queries
        self.assertEqual(len(queries), 22)
        db_connector.close()

    def test_generate_tpcds(self):
        self.db_name = "tpcds_test_db"

        TableGenerator(
            "tpcds",
            0.001,
            self.generating_connector,
            explicit_database_name=self.db_name,
        )

        db_connector = PostgresDatabaseConnector(self.db_name, autocommit=True)
        query_generator = QueryGenerator("tpcds", 1, db_connector, None, [])
        queries = query_generator.queries
        self.assertEqual(len(queries), 99)
        db_connector.close()

    def test_generate_job(self):
        self.db_name = "indexselection_job___1"

        # Loading the JOB tables takes some time,
        # we skip these tests if the dataset is not already loaded.
        if self.db_name not in self.generating_connector.database_names():
            return

        TableGenerator(
            "job", 1, self.generating_connector, explicit_database_name=self.db_name,
        )

        db_connector = PostgresDatabaseConnector(self.db_name, autocommit=True)

        # JOB supports only a scale factor of 1, i.e., no scaling
        with self.assertRaises(AssertionError):
            query_generator = QueryGenerator("job", 0.001, db_connector, None, [])

        # JOB does not support query filterting
        with self.assertRaises(AssertionError):
            query_generator = QueryGenerator(
                "job", 0.001, db_connector, query_ids=[17], columns=[]
            )

        query_generator = QueryGenerator("job", 1, db_connector, None, [])

        queries = query_generator.queries
        self.assertEqual(len(queries), 113)
        db_connector.close()

    def test_wrong_benchmark(self):
        with self.assertRaises(NotImplementedError):
            QueryGenerator("tpc-hallo", 1, self.generating_connector, None, [])


if __name__ == "__main__":
    unittest.main()
