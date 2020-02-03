from selection.dbms.postgres_dbms import PostgresDatabaseConnector
from selection.table_generator import TableGenerator
import unittest


class TestDatabase(unittest.TestCase):
    def setUp(self):
        self.db_name = 'indexselection_tpch___0_001'

    def test_postgres_connector(self):
        db = PostgresDatabaseConnector(None, autocommit=True)
        TableGenerator('tpch', 0.001, db)
        self.assertTrue(db)

    def test_postgres_index_simulation(self):
        db = PostgresDatabaseConnector(self.db_name, 'postgres')
        self.assertTrue(db.supports_index_simulation())

    def test_simple_statement(self):
        db = PostgresDatabaseConnector(None, autocommit=True)
        TableGenerator('tpch', 0.001, db)
        db = PostgresDatabaseConnector(self.db_name)
        statement = 'select count(*) from nation'
        result = db.exec_fetch(statement)
        self.assertEqual(result[0], 25)


if __name__ == '__main__':
    unittest.main()
