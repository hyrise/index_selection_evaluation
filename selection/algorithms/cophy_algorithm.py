from ..selection_algorithm import SelectionAlgorithm
from ..workload import Workload
from ..index import Index
import logging
import itertools
import time

# Algorithm stops when maximum number of indexes is reached
DEFAULT_PARAMETERS = {'max_index_columns': 2, 'max_indexes_per_query': 1}


class CoPhyAlgorithm(SelectionAlgorithm):
    def __init__(self, database_connector, parameters=None):
        if parameters is None:
            parameters = {}
        SelectionAlgorithm.__init__(self, database_connector, parameters,
                                    DEFAULT_PARAMETERS)

    def _calculate_best_indexes(self, workload):
        logging.info('Creating AMPL input for CoPhy')
        logging.info('Parameters: ' + str(self.parameters))

        time_start = time.time()
        COSTS_PER_QUERY_WITHOUT_INDEXES = {}
        for query in workload.queries:
            COSTS_PER_QUERY_WITHOUT_INDEXES[query] = self.cost_evaluation.calculate_cost(Workload([query], workload.database_name), set())

        accessed_columns_per_table = {}
        for query in workload.queries:
            for column in query.columns:
                if column.table not in accessed_columns_per_table:
                    accessed_columns_per_table[column.table] = set()
                accessed_columns_per_table[column.table].add(column)

        candidate_indexes = set()
        for number_of_index_columns in range(1, self.parameters['max_index_columns'] + 1):
            for table in accessed_columns_per_table:
                for index_columns in itertools.permutations(accessed_columns_per_table[table], number_of_index_columns):
                    candidate_indexes.add(Index(index_columns))

        # stores indexes that have a benefit in any combination (to prune indexes with no benefit)
        useful_indexes = set()
        costs_for_index_combination = {}

        for number_of_indexes_per_query in range(1, self.parameters['max_indexes_per_query'] + 1):
            for index_combination in itertools.combinations(candidate_indexes, number_of_indexes_per_query):
                is_useful_combination = False
                costs = []
                for query in workload.queries:
                    query_cost = self.cost_evaluation.calculate_cost(Workload([query], workload.database_name), set(index_combination), store_size=True)
                    costs.append(query_cost)
                    if query_cost < COSTS_PER_QUERY_WITHOUT_INDEXES[query]:
                        is_useful_combination = True
                if is_useful_combination:
                    costs_for_index_combination[index_combination] = costs
                    for index in index_combination:
                        useful_indexes.add(index)
        print(f'what-if time: {time.time() - time_start}')

        # generate AMPL input
        # sorted_useful_indexes = sorted(useful_indexes)

        # print size of index and determine index_ids, which are used in combinations
        index_ids = {}
        print('\nparam a :=')
        # print(0, 0)     # No index, no size
        for i, index in enumerate(sorted(useful_indexes)):
            assert(index.estimated_size), "Index size must be set."
            print(i + 1, index.estimated_size, f'# {index._column_names()}')
            index_ids[index] = i + 1

        # print index_ids per combination
        # combi 0 := no index
        print(f"set combi[0]:= ;")
        for i, index_combination in enumerate(costs_for_index_combination):
            index_id_list = [str(index_ids[index]) for index in index_combination]
            print(f"set combi[{i + 1}]:= {' '.join(index_id_list)};")

        # print costs per query and index_combination
        print('\nparam f4 :=')
        for query_id, query in enumerate(workload.queries):
            # Print cost without indexes
            print(query_id + 1, 0, COSTS_PER_QUERY_WITHOUT_INDEXES[query])
            for i, index_combination in enumerate(costs_for_index_combination):
                costs = costs_for_index_combination[index_combination]
                # print cost if they are lower than without indexes
                if costs[query_id] < COSTS_PER_QUERY_WITHOUT_INDEXES[query]:
                    print(query_id + 1, i + 1, costs[query_id])
        return []
