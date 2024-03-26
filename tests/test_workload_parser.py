import unittest

from selection.workload import Query, Workload
from selection.workload_parser import WorkloadParser


class TestWorkloadParser(unittest.TestCase):
    def test_execute(self):
        workload_parser = WorkloadParser("postgres", "indexselection_tpch___1", "example")
        workload = workload_parser.execute()

        self.assertEqual(type(workload), Worklaod)
        self.assertEqual(len(workload.queries), 1)
        query = workload.queries[0]
        self.assertEqual(type(query), Query)
        self.assertEqual(query.nr, "1.sql")
