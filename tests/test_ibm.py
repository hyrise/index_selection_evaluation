from selection.algorithms.ibm_algorithm import IBMAlgorithm, IndexBenefit
from selection.index import Index
from selection.workload import Column, Query, Table, Workload

import unittest
from unittest.mock import MagicMock
import time


class MockConnector:
    def __init__(self):
        pass

    def drop_indexes(self):
        pass

    def simulate_index(self, index):
        pass


MB_TO_BYTES = 1000000


class TestIBMAlgorithm(unittest.TestCase):
    def setUp(self):
        self.connector = MockConnector()
        self.algo = IBMAlgorithm(database_connector=self.connector)

        self.column_0 = Column("Col0")
        self.column_1 = Column("Col1")
        self.column_2 = Column("Col2")
        self.column_3 = Column("Col3")
        self.column_4 = Column("Col4")
        self.column_5 = Column("Col5")
        self.column_6 = Column("Col6")
        self.column_7 = Column("Col7")
        self.all_columns = [
            self.column_0,
            self.column_1,
            self.column_2,
            self.column_3,
            self.column_4,
            self.column_5,
            self.column_6,
            self.column_7,
        ]

        self.table = Table("Table0")
        self.table.add_columns(self.all_columns)

        self.query_0 = Query(
            0,
            "SELECT * FROM Table0 WHERE Col0 = 1 AND Col1 = 2 AND Col2 = 3;",
            self.all_columns,
        )
        # query_1 = Query(1, 'SELECT * FROM TableA WHERE ColA = 4;', [self.column_0])

    def test_ibm_algorithm(self):
        # Should use default parameters if none are specified
        budget_in_mb = 500
        self.assertEqual(self.algo.disk_constraint, budget_in_mb * MB_TO_BYTES)
        self.assertEqual(self.algo.cost_evaluation.cost_estimation, "whatif")
        self.assertEqual(self.algo.seconds_limit, 10)
        self.assertEqual(self.algo.maximum_remove, 4)

    def test_possible_indexes(self):
        column_0_table_1 = Column("Col0")
        table_1 = Table("Table1")
        table_1.add_column(column_0_table_1)
        query = Query(
            17,
            """SELECT * FROM Table0 as t0, Table1 as t1 WHERE t0.Col0 = 1"
                AND t0.Col1 = 2 AND t0.Col2 = 3 AND t1.Col0 = 17;""",
            [self.column_0, self.column_1, self.column_2, column_0_table_1],
        )
        indexes = self.algo._possible_indexes(query)
        self.assertIn(Index([column_0_table_1]), indexes)
        self.assertIn(Index([self.column_0]), indexes)
        self.assertIn(Index([self.column_1]), indexes)
        self.assertIn(Index([self.column_2]), indexes)
        self.assertIn(Index([self.column_0, self.column_1]), indexes)
        self.assertIn(Index([self.column_0, self.column_2]), indexes)
        self.assertIn(Index([self.column_1, self.column_0]), indexes)
        self.assertIn(Index([self.column_1, self.column_2]), indexes)
        self.assertIn(Index([self.column_2, self.column_0]), indexes)
        self.assertIn(Index([self.column_2, self.column_1]), indexes)
        self.assertIn(Index([self.column_0, self.column_1, self.column_2]), indexes)
        self.assertIn(Index([self.column_0, self.column_2, self.column_1]), indexes)
        self.assertIn(Index([self.column_1, self.column_0, self.column_2]), indexes)
        self.assertIn(Index([self.column_1, self.column_2, self.column_0]), indexes)
        self.assertIn(Index([self.column_2, self.column_0, self.column_1]), indexes)
        self.assertIn(Index([self.column_2, self.column_1, self.column_0]), indexes)

    def test_recommended_indexes(self):
        def _simulate_index_mock(index, store_size):
            index.hypopg_name = f"<1337>btree_{index.columns}"

        # For some reason, the database decides to only use an index for one of
        # the filters
        def _simulate_get_plan(query):
            plan = {
                "Total Cost": 17,
                "Plans": [
                    {
                        "Index Name": "<1337>btree_(C table0.col1,)",
                        "Filter": "(Col0 = 1)",
                    }
                ],
            }

            return plan

        query = Query(
            17,
            "SELECT * FROM Table0 WHERE Col0 = 1 AND Col1 = 2;",
            [self.column_0, self.column_1],
        )

        self.algo.database_connector.get_plan = MagicMock(side_effect=_simulate_get_plan)
        self.algo.what_if.simulate_index = MagicMock(side_effect=_simulate_index_mock)
        self.algo.what_if.drop_all_simulated_indexes = MagicMock()

        indexes, cost = self.algo._recommended_indexes(query)
        self.assertEqual(cost, 17)
        self.assertEqual(indexes, {Index([self.column_1])})

        self.assertEqual(self.algo.what_if.simulate_index.call_count, 4)
        self.algo.what_if.drop_all_simulated_indexes.assert_called_once()
        self.algo.database_connector.get_plan.assert_called_once_with(query)

    def test_exploit_virtual_indexes(self):
        def _simulate_index_mock(index, store_size):
            index.hypopg_name = f"<1337>btree_{index.columns}"

        # For some reason, the database decides to only use an index for one of
        # the filters
        def _simulate_get_plan(query):
            if "Table0" in query.text:
                return {
                    "Total Cost": 17,
                    "Plans": [{"Index Name": "<1337>btree_(C table0.col1,)"}],
                }

            return {"Total Cost": 5, "Plans": [{"Simple Table Retrieve": "table1"}]}

        query_0 = Query(
            0,
            "SELECT * FROM Table0 WHERE Col0 = 1 AND Col1 = 2;",
            [self.column_0, self.column_1],
        )
        query_1 = Query(1, "SELECT * FROM Table1;", [])
        workload = Workload([query_0, query_1], "database_name")

        self.algo.database_connector.get_plan = MagicMock(side_effect=_simulate_get_plan)
        self.algo.what_if.simulate_index = MagicMock(side_effect=_simulate_index_mock)
        self.algo.what_if.drop_all_simulated_indexes = MagicMock()
        query_results, index_candidates = self.algo._exploit_virtual_indexes(workload)
        self.assertEqual(len(query_results), len(workload.queries))
        expected_first_result = {
            "cost_without_indexes": 17,
            "cost_with_recommended_indexes": 17,
            "recommended_indexes": set([Index([self.column_1])]),
        }
        expected_second_result = {
            "cost_without_indexes": 5,
            "cost_with_recommended_indexes": 5,
            "recommended_indexes": set(),
        }
        self.assertEqual(query_results[query_0], expected_first_result)
        self.assertEqual(query_results[query_1], expected_second_result)
        self.assertEqual(index_candidates, set([Index([self.column_1])]))

    def test_calculate_index_benefits(self):
        index_0 = Index([self.column_0])
        index_0.estimated_size = 5
        index_1 = Index([self.column_1])
        index_1.estimated_size = 1
        index_2 = Index([self.column_2])
        index_2.estimated_size = 3

        query_result_0 = {
            "cost_without_indexes": 100,
            "cost_with_recommended_indexes": 50,
            "recommended_indexes": [index_0, index_1],
        }
        # Yes, negative benefit is possible
        query_result_1 = {
            "cost_without_indexes": 50,
            "cost_with_recommended_indexes": 60,
            "recommended_indexes": [index_1],
        }
        query_result_2 = {
            "cost_without_indexes": 60,
            "cost_with_recommended_indexes": 57,
            "recommended_indexes": [index_2],
        }
        query_result_3 = {
            "cost_without_indexes": 60,
            "cost_with_recommended_indexes": 60,
            "recommended_indexes": [],
        }
        query_results = {
            "q0": query_result_0,
            "q1": query_result_1,
            "q2": query_result_2,
            "q3": query_result_3,
        }

        index_benefits = self.algo._calculate_index_benefits(
            [index_0, index_1, index_2], query_results
        )
        expected_index_benefits = [
            IndexBenefit(index_1, 40),
            IndexBenefit(index_0, 50),
            IndexBenefit(index_2, 3),
        ]

        self.assertEqual(index_benefits, expected_index_benefits)

    def test_combine_subsumed(self):
        index_0_1 = Index([self.column_0, self.column_1])
        index_0_1.estimated_size = 2
        index_0 = Index([self.column_0])
        index_0.estimated_size = 1
        index_1 = Index([self.column_1])
        index_1.estimated_size = 1

        # Scenario 1. Index subsumed because better ratio for larger index
        index_benefits = [IndexBenefit(index_0_1, 21), IndexBenefit(index_0, 10)]
        subsumed = self.algo._combine_subsumed(index_benefits)
        expected = [IndexBenefit(index_0_1, 31)]
        self.assertEqual(subsumed, expected)

        # Scenario 2. Index not subsumed because better index has fewer attributes
        index_benefits = [IndexBenefit(index_0, 11), IndexBenefit(index_0_1, 20)]
        subsumed = self.algo._combine_subsumed(index_benefits)
        expected = [IndexBenefit(index_0, 11), IndexBenefit(index_0_1, 20)]
        self.assertEqual(subsumed, expected)

        # Scenario 3. Index not subsumed because last element does not match
        # attribute even though better ratio
        index_0_1_2 = Index([self.column_0, self.column_1, self.column_2])
        index_0_1_2.estimated_size = 3
        index_0_2 = Index([self.column_0, self.column_2])
        index_0_2.estimated_size = 2

        index_benefits = [IndexBenefit(index_0_1_2, 31), IndexBenefit(index_0_2, 20)]
        subsumed = self.algo._combine_subsumed(index_benefits)
        expected = [IndexBenefit(index_0_1_2, 31), IndexBenefit(index_0_2, 20)]
        self.assertEqual(subsumed, expected)

        # Scenario 4. Multi Index subsumed
        index_benefits = [IndexBenefit(index_0_1_2, 31), IndexBenefit(index_0_1, 20)]
        subsumed = self.algo._combine_subsumed(index_benefits)
        expected = [IndexBenefit(index_0_1_2, 51)]
        self.assertEqual(subsumed, expected)

        # Scenario 5. Multiple Indexes subsumed
        index_benefits = [
            IndexBenefit(index_0_1_2, 31),
            IndexBenefit(index_0_1, 20),
            IndexBenefit(index_0, 10),
        ]
        subsumed = self.algo._combine_subsumed(index_benefits)
        expected = [IndexBenefit(index_0_1_2, 61)]
        self.assertEqual(subsumed, expected)

        # Scenario 6. Input returned if len(input) < 2
        subsumed = self.algo._combine_subsumed([IndexBenefit(index_0_1, 21)])
        expected = [IndexBenefit(index_0_1, 21)]
        self.assertEqual(subsumed, expected)

        # Scenario 7. Input not sorted by ratio throws
        with self.assertRaises(AssertionError):
            subsumed = self.algo._combine_subsumed(
                [IndexBenefit(index_0, 10), IndexBenefit(index_0_1, 21)]
            )

    def test_evaluate_workload(self):
        index_0 = Index([self.column_0])
        index_1 = Index([self.column_1])
        self.algo.cost_evaluation.calculate_cost = MagicMock()

        self.algo._evaluate_workload(
            [IndexBenefit(index_0, 10), IndexBenefit(index_1, 9)], workload=[]
        )
        self.algo.cost_evaluation.calculate_cost.assert_called_once_with(
            [], [index_0, index_1]
        )

    def test_try_variations_time_limit(self):
        index_0 = Index([self.column_0])
        index_0.estimated_size = 1
        index_1 = Index([self.column_1])
        index_1.estimated_size = 1
        index_2 = Index([self.column_2])
        index_2.estimated_size = 1
        index_3 = Index([self.column_3])
        index_3.estimated_size = 1
        index_4 = Index([self.column_4])
        index_4.estimated_size = 1
        index_5 = Index([self.column_5])
        index_5.estimated_size = 1
        index_6 = Index([self.column_6])
        index_6.estimated_size = 1
        index_7 = Index([self.column_7])
        index_7.estimated_size = 5
        self.algo.cost_evaluation.calculate_cost = MagicMock(return_value=17)
        self.algo.seconds_limit = 0.2

        time_before = time.time()
        self.algo._try_variations(
            selected_index_benefits=frozenset([IndexBenefit(index_0, 1)]),
            index_benefits=frozenset([IndexBenefit(index_1, 1)]),
            workload=[],
        )
        self.assertGreaterEqual(time.time(), time_before + self.algo.seconds_limit)

        def fake(selected, workload):
            cost = 10
            if IndexBenefit(index_3, 1.5) in selected:
                cost -= 0.5
            if IndexBenefit(index_4, 0.5) in selected:
                cost += 0.5
            if IndexBenefit(index_1, 0.5) in selected:
                cost += 0.5
            if IndexBenefit(index_1, 0.5) in selected:
                cost += 0.5
            return cost

        # In this scenario a good index has not been selected (index_3).
        # We test three things:
        # (i)   That index_3 gets chosen by variation.
        # (ii)  That the weakest index from the original selection gets
        #         removed (index_1).
        # (iii) That index_4 does not get chosen even though it is better than index_1.
        self.algo._evaluate_workload = fake
        self.algo.maximum_remove = 1
        self.algo.disk_constraint = 3
        new = self.algo._try_variations(
            selected_index_benefits=frozenset(
                [
                    IndexBenefit(index_0, 1),
                    IndexBenefit(index_1, 0.5),
                    IndexBenefit(index_2, 1),
                ]
            ),
            index_benefits=frozenset(
                [
                    IndexBenefit(index_0, 1),
                    IndexBenefit(index_1, 0.5),
                    IndexBenefit(index_2, 1),
                    IndexBenefit(index_3, 1.5),
                    IndexBenefit(index_4, 0.6),
                ]
            ),
            workload=[],
        )
        self.assertIn(IndexBenefit(index_3, 1.5), new)
        self.assertNotIn(IndexBenefit(index_4, 0.5), new)
        self.assertNotIn(IndexBenefit(index_1, 0.5), new)

        # Test that good index is not chosen because of storage restrictions
        new = self.algo._try_variations(
            selected_index_benefits=frozenset([IndexBenefit(index_0, 1)]),
            index_benefits=frozenset(
                [IndexBenefit(index_0, 1), IndexBenefit(index_7, 5)]
            ),
            workload=[],
        )
        self.assertEqual(new, set([IndexBenefit(index_0, 1)]))
