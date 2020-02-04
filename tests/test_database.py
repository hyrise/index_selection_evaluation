from selection.dbms.postgres_dbms import PostgresDatabaseConnector
from selection.table_generator import TableGenerator
import unittest


class TestDatabase(unittest.TestCase):
    def setUp(self):
        self.db_name = 'indexselection_tpch___0_001'

    def teatDown(self):
        connector = PostgresDatabaseConnector(None,
                                                      autocommit=True)

        dbs = ['indexselection_tpch___0_001']
        for db in dbs:
            if connector.database_exists(db):
                connector.drop_database(db)


    def test_postgres_index_simulation(self):
        db = PostgresDatabaseConnector(None, autocommit=True)
        table_generator = TableGenerator('tpch', 0.001, db)
        db.close()

        db = PostgresDatabaseConnector(table_generator.database_name(), 'postgres')
        self.assertTrue(db.supports_index_simulation())
        db.close()

    def test_simple_statement(self):
        db = PostgresDatabaseConnector(None, autocommit=True)
        table_generator = TableGenerator('tpch', 0.001, db)
        db.close()

        db = PostgresDatabaseConnector(table_generator.database_name())

        statement = 'select count(*) from nation'
        result = db.exec_fetch(statement)
        self.assertEqual(result[0], 25)

        db.close()


if __name__ == '__main__':
    unittest.main()
