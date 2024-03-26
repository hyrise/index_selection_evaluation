import unittest

from selection.workload import Column, Query, Table, Workload
from selection.workload_parser import WorkloadParser


class TestWorkloadParser(unittest.TestCase):
    def test_execute(self):
        workload_parser = WorkloadParser("postgres", "indexselection_tpch___1", "example")
        workload = workload_parser.execute()

        self.assertEqual(type(workload), Workload)
        self.assertEqual(len(workload.queries), 2)
        query = workload.queries[0]
        self.assertEqual(type(query), Query)
        self.assertEqual(query.nr, "1.sql")
        self.assertEqual(len(query.columns), 1)

        query = workload.queries[1]
        self.assertEqual(len(query.columns), 3)

    def test_get_tables(self):
        workload_parser = WorkloadParser("postgres", "indexselection_tpch___1", "example")
        tables = workload_parser.get_tables()

        self.assertEqual(len(tables), 8)
        table_orders = tables["orders"]
        self.assertEqual(type(table_orders), Table)
        orders_columns = table_orders.columns
        self.assertEqual(len(orders_columns), 9)
        self.assertEqual(type(orders_columns[0]), Column)
        self.assertEqual(orders_columns[0].name, "o_orderkey")

    def test_is_custom_workload(self):
        self.assertEqual(WorkloadParser.is_custom_workload("tpch"), False)
        self.assertEqual(WorkloadParser.is_custom_workload("tpcds"), False)
        self.assertEqual(WorkloadParser.is_custom_workload("example"), True)
