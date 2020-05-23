from ..selection_algorithm import SelectionAlgorithm
from ..what_if_index_creation import WhatIfIndexCreation
from ..index import Index

import itertools
import logging

# Maximum number of columns per index, storage budget in MB,
DEFAULT_PARAMETERS = {
    "max_index_columns": 3,
    "budget": 500,
}


class RelaxationAlgorithm(SelectionAlgorithm):
    def __init__(self, database_connector, parameters=None):
        if parameters is None:
            parameters = {}
        SelectionAlgorithm.__init__(
            self, database_connector, parameters, DEFAULT_PARAMETERS
        )
        self.what_if = WhatIfIndexCreation(database_connector)
        # convert MB to bytes
        self.disk_constraint = self.parameters["budget"] * 1000000

    def _calculate_best_indexes(self, workload):
        logging.info("Calculating best indexes Relaxation")
        # Obtain best indexes per query
        _, candidates = self._exploit_virtual_indexes(workload)

        # CP in Figure 5
        cp = candidates.copy()
        cp_size = sum(index.estimated_size for index in cp)
        cp_cost = self.cost_evaluation.calculate_cost(workload, cp, store_size=True)
        while cp_size > self.disk_constraint:
            # Pick a configuration that can be relaxed
            # TODO: Currently only one is considered

            # Relax the configuration
            # TODO: Currently a random index is removed
            best_relaxed = None
            best_relaxed_size = None
            lowest_relaxed_penalty = None

            # removal
            for index in cp:
                relaxed = cp.copy()
                relaxed.remove(index)
                relaxed_cost = self.cost_evaluation.calculate_cost(workload, relaxed, store_size=True)
                relaxed_cost_increase = relaxed_cost - cp_cost
                assert relaxed_cost_increase >= 0, 'Relaxed cost increase must be positive'
                # any storage decrease beyond the disk_constraint is not considered
                relaxed_storage_savings = min(index.estimated_size, cp_size - self.disk_constraint)
                assert relaxed_storage_savings > 0, 'Relaxed storage savings must be positive'

                if best_relaxed is None or lowest_relaxed_penalty > (relaxed_cost_increase / relaxed_storage_savings):
                    # set new best relaxed configuration
                    best_relaxed = relaxed
                    best_relaxed_size = cp_size - index.estimated_size
                    lowest_relaxed_penalty = (relaxed_cost_increase / relaxed_storage_savings)

            cp = best_relaxed
            cp_size = best_relaxed_size

        return list(cp)

    # copied from IBMAlgorithm
    def _exploit_virtual_indexes(self, workload):
        query_results = {}
        index_candidates = set()
        for query in workload.queries:
            plan = self.database_connector.get_plan(query)
            cost_without_indexes = plan["Total Cost"]
            (
                recommended_indexes,
                cost_with_recommended_indexes,
            ) = self._recommended_indexes(query)
            query_results[query] = {
                "cost_without_indexes": cost_without_indexes,
                "cost_with_recommended_indexes": cost_with_recommended_indexes,
                "recommended_indexes": recommended_indexes,
            }
            index_candidates |= recommended_indexes
        return query_results, index_candidates

    # copied from IBMAlgorithm
    def _recommended_indexes(self, query):
        """Simulates all possible indexes for the query and returns the used one"""
        logging.debug("Simulating indexes")

        possible_indexes = self._possible_indexes(query)
        for index in possible_indexes:
            self.what_if.simulate_index(index, store_size=True)

        plan = self.database_connector.get_plan(query)
        plan_string = str(plan)
        cost = plan["Total Cost"]

        self.what_if.drop_all_simulated_indexes()

        recommended_indexes = set()
        for index in possible_indexes:
            if index.hypopg_name in plan_string:
                recommended_indexes.add(index)

        logging.debug(f"Recommended indexes found: {len(recommended_indexes)}")
        return recommended_indexes, cost

    # copied from IBMAlgorithm
    def _possible_indexes(self, query):
        # "SAEFIS" or "BFI" see IBM paper
        # This implementation is "BFI"
        columns = query.columns
        logging.debug(f"\n{query}")
        logging.debug(f"indexable columns: {len(columns)}")
        max_columns = self.parameters["max_index_columns"]

        indexable_columns_per_table = {}
        for column in columns:
            if column.table not in indexable_columns_per_table:
                indexable_columns_per_table[column.table] = set()
            indexable_columns_per_table[column.table].add(column)

        possible_column_combinations = set()
        for table in indexable_columns_per_table:
            columns = indexable_columns_per_table[table]
            for index_length in range(1, max_columns + 1):
                possible_column_combinations |= set(
                    itertools.permutations(columns, index_length)
                )

        logging.debug(f"possible indexes: {len(possible_column_combinations)}")
        return [Index(p) for p in possible_column_combinations]
