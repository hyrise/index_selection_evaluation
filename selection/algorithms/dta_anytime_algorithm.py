from ..selection_algorithm import SelectionAlgorithm
from ..what_if_index_creation import WhatIfIndexCreation
from ..index import Index, index_merge

import itertools
import logging
import math
import time

# Maximum number of columns per index, storage budget in MB, runtime limit.
# After n minutes the algorithm is stopped and the current best solution is returned.
DEFAULT_PARAMETERS = {"max_index_columns": 3, "budget": 500, "max_runtime_minutes": 10}


class DTAAnytimeAlgorithm(SelectionAlgorithm):
    def __init__(self, database_connector, parameters=None):
        if parameters is None:
            parameters = {}
        SelectionAlgorithm.__init__(
            self, database_connector, parameters, DEFAULT_PARAMETERS
        )
        self.what_if = WhatIfIndexCreation(database_connector)
        # convert MB to bytes
        self.disk_constraint = self.parameters["budget"] * 1000000
        self.max_index_columns = self.parameters["max_index_columns"]
        self.max_runtime_minutes = self.parameters["max_runtime_minutes"]

    def _calculate_best_indexes(self, workload):
        logging.info("Calculating best indexes Relaxation")
        # Obtain best indexes per query
        _, candidates = self._exploit_virtual_indexes(workload)
        self._add_merged_indexes(candidates)

        # Remove candidates that cannot meet budget requirements
        seeds = []
        filtered_candidates = set()
        for candidate in candidates:
            if candidate.estimated_size > self.disk_constraint:
                continue

            seeds.append({candidate})
            filtered_candidates.add(candidate)
        seeds.append(set())

        candidates = filtered_candidates

        start_time = time.time()
        best_configuration = (None, None)
        for i, seed in enumerate(seeds):
            logging.info(f"Seed {i + 1} from {len(seeds)}")
            candidates_copy = candidates.copy()
            candidates_copy -= seed
            current_costs = self._simulate_and_evaluate_cost(workload, seed)
            indexes, costs = self.enumerate_greedy(
                workload, seed, current_costs, candidates_copy, math.inf
            )
            if best_configuration[0] is None or costs < best_configuration[1]:
                best_configuration = (indexes, costs)

            current_time = time.time()
            consumed_time = current_time - start_time
            if consumed_time > self.max_runtime_minutes * 60:
                logging.info(
                    f"Stopping after {i + 1} seeds because of timing constraints."
                )
                break
            else:
                logging.debug(
                    f"Current best: {best_configuration[1]} after {consumed_time}s."
                )

        indexes = best_configuration[0]
        return list(indexes)

    # copied from RelaxationAlgorithm
    def _indexes_by_table(self, configuration):
        indexes_by_table = {}
        for index in configuration:
            table = index.table()
            if table not in indexes_by_table:
                indexes_by_table[table] = []

            indexes_by_table[table].append(index)

        return indexes_by_table

    def _add_merged_indexes(self, indexes):
        indexes_by_table = self._indexes_by_table(indexes)
        for table in indexes_by_table:
            for index1, index2 in itertools.permutations(indexes_by_table[table], 2):
                merged_index = index_merge(index1, index2)
                if len(merged_index.columns) > self.max_index_columns:
                    new_columns = merged_index.columns[: self.max_index_columns]
                    merged_index = Index(new_columns)
                if merged_index not in indexes:
                    self.cost_evaluation.estimate_size(merged_index)
                    indexes.add(merged_index)

    # based on MicrosoftAlgorithm
    def enumerate_greedy(
        self, workload, current_indexes, current_costs, candidate_indexes, number_indexes,
    ):
        assert (
            current_indexes & candidate_indexes == set()
        ), "Intersection of current and candidate indexes must be empty"
        if len(current_indexes) >= number_indexes:
            return current_indexes, current_costs

        # (index, cost)
        best_index = (None, None)

        logging.debug(f"Searching in {len(candidate_indexes)} indexes")

        for index in candidate_indexes:
            if (
                sum(idx.estimated_size for idx in current_indexes | {index})
                > self.disk_constraint
            ):
                # index configuration is too large
                continue
            cost = self._simulate_and_evaluate_cost(workload, current_indexes | {index})

            if not best_index[0] or cost < best_index[1]:
                best_index = (index, cost)
        if best_index[0] and best_index[1] < current_costs:
            current_indexes.add(best_index[0])
            candidate_indexes.remove(best_index[0])
            current_costs = best_index[1]

            logging.debug(f"Additional best index found: {best_index}")

            return self.enumerate_greedy(
                workload,
                current_indexes,
                current_costs,
                candidate_indexes,
                number_indexes,
            )
        return current_indexes, current_costs

    # copied from MicrosoftAlgorithm
    def _simulate_and_evaluate_cost(self, workload, indexes):
        cost = self.cost_evaluation.calculate_cost(workload, indexes, store_size=True)
        return round(cost, 2)

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
