from selection.table_generator import TableGenerator
from selection.dbms.postgres_dbms import PostgresDatabaseConnector
from selection.workload import Column, Table
import unittest


class TestTableGenerator(unittest.TestCase):
    def setUp(self):
        self.generating_connector = PostgresDatabaseConnector(None,
                                                      autocommit=True)

    def test_database_name(self):
        table_generator = TableGenerator('tpch', 0.001, self.generating_connector)
        self.assertEqual(table_generator.database_name(),
            'indexselection_tpch___0_001')

        table_generator = TableGenerator('tpcds', 0.001, self.generating_connector)
        self.assertEqual(table_generator.database_name(),
            'indexselection_tpcds___0_001')

        table_generator = TableGenerator('tpch', 0.001, self.generating_connector, explicit_database_name="test_db")
        self.assertEqual(table_generator.database_name(),
            'test_db')

    def test_generate_tpch(self):
        table_generator = TableGenerator('tpch', 0.001, self.generating_connector)

        # Check that lineitem table exists in TableGenerator
        lineitem_table = None
        for table in table_generator.tables:
            if table.name == 'lineitem':
                lineitem_table = table
                break
        self.assertTrue(lineitem_table is not None)

        # Check that l_receiptdate column exists in TableGenerator and table
        l_receiptdate = Column('l_receiptdate', lineitem_table)
        self.assertTrue(l_receiptdate in table_generator.columns)
        self.assertTrue(l_receiptdate in table.columns)

        database_connect = PostgresDatabaseConnector(table_generator.database_name(), autocommit=True)

        tpch_tables = ['customer', 'lineitem', 'nation', 'orders', 'part', 'partsupp', 'region', 'supplier']
        for tpch_table in tpch_tables:
            self.assertTrue(database_connect.table_exists(tpch_table))

    def test_generate_tpds(self):
        table_generator = TableGenerator('tpcds', 0.001, self.generating_connector)

        # Check that lineitem table exists in TableGenerator
        item_table = None
        for table in table_generator.tables:
            if table.name == 'item':
                item_table = table
                break
        self.assertTrue(item_table is not None)

        # Check that l_receiptdate column exists in TableGenerator and table
        i_item_sk = Column('i_item_sk', item_table)
        self.assertTrue(i_item_sk in table_generator.columns)
        self.assertTrue(i_item_sk in table.columns)

        database_connect = PostgresDatabaseConnector(table_generator.database_name(), autocommit=True)

        tpcds_tables = ['call_center', 'catalog_page', 'catalog_returns', 'catalog_sales', 'customer',
            'customer_address', 'customer_demographics', 'date_dim', "household_demographics", "income_band",
            "inventory", "item", "promotion", "reason", "ship_mode", "store", "store_returns", "store_sales",
            "time_dim", "warehouse", "web_page", "web_returns", "web_sales", "web_site"]
        for tpcds_table in tpcds_tables:
            self.assertTrue(database_connect.table_exists(tpcds_table))

    def test_not_implemented(self):
        with self.assertRaises(NotImplementedError):
            TableGenerator('not_tpch', 0.001, self.generating_connector)

    def test_tpcds_with_wrong_sf(self):
        with self.assertRaises(Exception):
            TableGenerator('tpcds', 0.002, self.generating_connector)


if __name__ == '__main__':
    unittest.main()
