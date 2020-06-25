import unittest

from selection.dbms.postgres_dbms import PostgresDatabaseConnector
from selection.table_generator import TableGenerator
from selection.workload import Column


class TestTableGenerator(unittest.TestCase):
    def setUp(self):
        self.generating_connector = PostgresDatabaseConnector(None, autocommit=True)

    def tearDown(self):
        connector = PostgresDatabaseConnector(None, autocommit=True)

        dbs = ["indexselection_tpch___0_001", "indexselection_tpcds___0_001", "test_db"]
        for db in dbs:
            if connector.database_exists(db):
                connector.drop_database(db)

    def test_database_name(self):
        table_generator = TableGenerator("tpch", 0.001, self.generating_connector)
        self.assertEqual(table_generator.database_name(), "indexselection_tpch___0_001")
        self.assertTrue(
            self.generating_connector.database_exists("indexselection_tpch___0_001")
        )

        table_generator = TableGenerator("tpcds", 0.001, self.generating_connector)
        self.assertEqual(table_generator.database_name(), "indexselection_tpcds___0_001")
        self.assertTrue(
            self.generating_connector.database_exists("indexselection_tpcds___0_001")
        )

        table_generator = TableGenerator(
            "tpch", 0.001, self.generating_connector, explicit_database_name="test_db"
        )
        self.assertEqual(table_generator.database_name(), "test_db")
        self.assertTrue(self.generating_connector.database_exists("test_db"))

        self.generating_connector.close()

    def test_generate_tpch(self):
        table_generator = TableGenerator("tpch", 0.001, self.generating_connector)

        # Check that correct number of columns were extracted
        self.assertEqual(61, len(table_generator.columns))

        # Check that lineitem table exists in TableGenerator
        lineitem_table = None
        for table in table_generator.tables:
            if table.name == "lineitem":
                lineitem_table = table
                break
        self.assertIsNotNone(lineitem_table)

        # Check that l_receiptdate column exists in TableGenerator and Table object
        l_receiptdate = Column("l_receiptdate")
        l_receiptdate.table = lineitem_table
        self.assertIn(l_receiptdate, table_generator.columns)
        self.assertIn(l_receiptdate, lineitem_table.columns)

        database_connect = PostgresDatabaseConnector(
            table_generator.database_name(), autocommit=True
        )

        tpch_tables = [
            "customer",
            "lineitem",
            "nation",
            "orders",
            "part",
            "partsupp",
            "region",
            "supplier",
        ]
        for tpch_table in tpch_tables:
            self.assertTrue(database_connect.table_exists(tpch_table))

        self.generating_connector.close()
        database_connect.close()

    def test_generate_tpcds(self):
        table_generator = TableGenerator("tpcds", 0.001, self.generating_connector)

        # Check that correct number of columns were extracted
        self.assertEqual(429, len(table_generator.columns))

        # Check that item table exists in TableGenerator
        item_table = None
        for table in table_generator.tables:
            if table.name == "item":
                item_table = table
                break
        self.assertIsNotNone(item_table)

        # Check that i_item_sk column exists in TableGenerator and Table object
        i_item_sk = Column("i_item_sk")
        i_item_sk.table = item_table
        self.assertIn(i_item_sk, table_generator.columns)
        self.assertIn(i_item_sk, item_table.columns)

        database_connect = PostgresDatabaseConnector(
            table_generator.database_name(), autocommit=True
        )

        tpcds_tables = [
            "call_center",
            "catalog_page",
            "catalog_returns",
            "catalog_sales",
            "customer",
            "customer_address",
            "customer_demographics",
            "date_dim",
            "household_demographics",
            "income_band",
            "inventory",
            "item",
            "promotion",
            "reason",
            "ship_mode",
            "store",
            "store_returns",
            "store_sales",
            "time_dim",
            "warehouse",
            "web_page",
            "web_returns",
            "web_sales",
            "web_site",
        ]
        for tpcds_table in tpcds_tables:
            self.assertTrue(database_connect.table_exists(tpcds_table))

        self.generating_connector.close()
        database_connect.close()

    def test_generate_job(self):
        # Loading the JOB tables takes some time, we skip tests if the dataset is not already loaded.
        if "indexselection_job___1" not in self.generating_connector.database_names():
            return

        # JOB supports only a scale factor of 1, i.e., no scaling
        with self.assertRaises(AssertionError):
            table_generator = TableGenerator("job", 0.001, self.generating_connector)

        table_generator = TableGenerator("job", 1, self.generating_connector)

        # Check that correct number of columns were extracted
        self.assertEqual(108, len(table_generator.columns))

        # Check that item table exists in TableGenerator
        title_table = None
        for table in table_generator.tables:
            if table.name == "title":
                title_table = table
                break
        self.assertIsNotNone(title_table)

        # Check that i_item_sk column exists in TableGenerator and Table object
        imdb_index = Column("imdb_index")
        imdb_index.table = title_table
        self.assertIn(imdb_index, table_generator.columns)
        self.assertIn(imdb_index, title_table.columns)

        database_connect = PostgresDatabaseConnector(
            table_generator.database_name(), autocommit=True
        )

        job_tables = [
            "aka_name",
"aka_title",
"cast_info",
"char_name",
"comp_cast_type",
"company_name",
"company_type",
"complete_cast",
"info_type",
"keyword",
"kind_type",
"link_type",
"movie_companies",
"movie_info",
"movie_info_idx",
"movie_keyword",
"movie_link",
"name",
"person_info",
"role_type",
"title",
        ]
        for job_table in job_tables:
            self.assertTrue(database_connect.table_exists(job_table))

        self.generating_connector.close()
        database_connect.close()

    def test_not_implemented(self):
        with self.assertRaises(NotImplementedError):
            TableGenerator("not_tpch", 0.001, self.generating_connector)

    def test_tpcds_with_wrong_sf(self):
        with self.assertRaises(Exception):
            TableGenerator("tpcds", 0.002, self.generating_connector)


if __name__ == "__main__":
    unittest.main()
