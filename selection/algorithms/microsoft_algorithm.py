from ..selection_algorithm import SelectionAlgorithm
from ..workload import Workload
from ..index import Index
import itertools
import logging

# multi column index methods: 'no', 'lead', 'all'
# cost_estimation: 'whatif' or 'acutal_runtimes'
DEFAULT_PARAMETERS = {
    "max_indexes": 15,
    "max_indexes_naive": 3,
    "max_index_columns": 2,
    "cost_estimation": "whatif",
}


class MicrosoftAlgorithm(SelectionAlgorithm):
    def __init__(self, database_connector, parameters):
        SelectionAlgorithm.__init__(
            self, database_connector, parameters, DEFAULT_PARAMETERS
        )
        self.max_indexes = self.parameters["max_indexes"]
        self.max_indexes_naive = min(
            self.parameters["max_indexes_naive"], self.max_indexes
        )
        self.max_columns_per_index = self.parameters["max_index_columns"]

    def _calculate_best_indexes(self, workload):
        logging.info("Calculating best indexes (microsoft)")
        logging.info("Parameters: " + str(self.parameters))

        if self.max_indexes == 0:
            return []

        # Set potential indexes for first iteration
        potential_indexes = workload.potential_indexes()
        for current_max_columns_per_index in range(1, self.max_columns_per_index + 1):
            candidates = self.select_index_candidates(workload, potential_indexes)
            indexes = self.enumerate_combinations(workload, candidates)
            assert indexes <= candidates, "Indexes must be a subset of candidate indexes"

            if current_max_columns_per_index < self.max_columns_per_index:
                # Update potential indexes for the next iteration
                potential_indexes = indexes | self.create_multicolumn_indexes(
                    workload, indexes
                )
        return indexes

    def select_index_candidates(self, workload, potential_indexes):
        candidates = set()

        for query in workload.queries:
            logging.debug(f"Find candidates for query\t{query}...")
            # Create a workload consisting of one query
            query_workload = Workload([query], workload.database_name)
            indexes = self._potential_indexes_for_query(query, potential_indexes)
            candidates |= self.enumerate_combinations(query_workload, indexes)

        logging.info(
            f"Number of candidates: {len(candidates)}\n" f"Candidates: {candidates}"
        )
        return candidates

    def _potential_indexes_for_query(self, query, potential_indexes):
        indexes = set()
        for index in potential_indexes:
            # The leading index column must be referenced by the query
            if index.columns[0] in query.columns:
                indexes.add(index)
        return indexes

    def enumerate_combinations(self, workload, candidate_indexes):
        log_out = (
            f"Start Enumeration\n"
            f"\tNumber of candidate indexes: {len(candidate_indexes)}\n"
            f"\tNumber of indexes to be selected: {self.max_indexes}"
        )
        logging.debug(log_out)

        number_indexes_naive = min(self.max_indexes_naive, len(candidate_indexes))
        current_indexes, costs = self.enumerate_naive(
            workload, candidate_indexes, number_indexes_naive
        )

        log_out = (
            f"lowest cost (naive): {costs}\n"
            f"\tlowest cost indexes (naive): {current_indexes}"
        )
        logging.debug(log_out)

        number_indexes = min(self.max_indexes, len(candidate_indexes))
        indexes, costs = self.enumerate_greedy(
            workload,
            current_indexes,
            costs,
            candidate_indexes - current_indexes,
            number_indexes,
        )

        log_out = (
            f"lowest cost (greedy): {costs}\n"
            f"\tlowest cost indexes (greedy): {indexes}\n"
            f"(greedy): number indexes {len(indexes)}\n"
        )
        logging.debug(log_out)

        return set(indexes)

    def enumerate_naive(self, workload, candidate_indexes, number_indexes_naive):
        lowest_cost_indexes = set()
        lowest_cost = None

        for number_of_indexes in range(1, number_indexes_naive + 1):
            for index_combination in itertools.combinations(
                candidate_indexes, number_of_indexes
            ):
                cost = self._simulate_and_evaluate_cost(workload, index_combination)
                if not lowest_cost or cost < lowest_cost:
                    lowest_cost_indexes = index_combination
                    lowest_cost = cost

        return set(lowest_cost_indexes), lowest_cost

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

    def _simulate_and_evaluate_cost(self, workload, indexes):
        cost = self.cost_evaluation.calculate_cost(workload, indexes, store_size=True)
        return round(cost, 2)

    def create_multicolumn_indexes(self, workload, indexes):
        multicolumn_candidates = set()
        for index in indexes:
            # Extend the index with all indexable columns of the same table,
            # that are not already part of the index
            for column in (
                set(index.table().columns) & set(workload.indexable_columns())
            ) - set(index.columns):
                multicolumn_candidates.add(Index(index.columns + (column,)))
        return multicolumn_candidates
