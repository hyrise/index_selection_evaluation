from selection.dbms.postgres_dbms import PostgresDatabaseConnector
from selection.table_generator import TableGenerator
import unittest


class TestDatabase(unittest.TestCase):
    @classmethod
    def setUpClass(self):
        self.db_name = 'tpch_test_db_database'
        db = PostgresDatabaseConnector(None, autocommit=True)
        table_generator = TableGenerator('tpch', 0.001, db, explicit_database_name=self.db_name)
        db.close()

    @classmethod
    def tearDownClass(self):
        connector = PostgresDatabaseConnector(None,
                                                      autocommit=True)
        if connector.database_exists(self.db_name):
            connector.drop_database(self.db_name)


    def test_postgres_index_simulation(self):
        db = PostgresDatabaseConnector(self.db_name, 'postgres')
        self.assertTrue(db.supports_index_simulation())
        db.close()

    def test_simple_statement(self):
        db = PostgresDatabaseConnector(self.db_name, 'postgres')

        statement = 'select count(*) from nation'
        result = db.exec_fetch(statement)
        self.assertEqual(result[0], 25)

        db.close()


if __name__ == '__main__':
    unittest.main()
