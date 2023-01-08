import os
from typing import Any, Dict, Set
from selection.algorithms.anytime_algorithm import AnytimeAlgorithm
from selection.algorithms.auto_admin_algorithm import AutoAdminAlgorithm
from selection.algorithms.cophy_algorithm import CoPhyAlgorithm
from selection.algorithms.db2advis_algorithm import DB2AdvisAlgorithm
from selection.algorithms.dexter_algorithm import DexterAlgorithm
from selection.algorithms.drop_heuristic_algorithm import DropHeuristicAlgorithm

from selection.algorithms.extend_algorithm import ExtendAlgorithm
from selection.algorithms.relaxation_algorithm import RelaxationAlgorithm
from ..selection_algorithm import AllIndexesAlgorithm, SelectionAlgorithm
from ..workload import Workload
from ..index import Index
import logging
import itertools
import time

# The maximum width of index candidates and the number of applicable indexes per query can be specified
DEFAULT_PARAMETERS = {"max_index_width": 2, "max_indexes_per_query": 1, "target_path": 'cophy_data_files', 'benchmark': '', 'extra_algorithms': 'none', 'overwrite': False}

class CoPhyExpandedAlgorithm(SelectionAlgorithm):
    def __init__(self, database_connector, parameters=None):
        if parameters is None:
            parameters = {}
        SelectionAlgorithm.__init__(self, database_connector, parameters,
                                    DEFAULT_PARAMETERS)

    def _calculate_best_indexes(self, workload: Workload):
        logging.info('Creating AMPL input for CoPhy')
        logging.info('Parameters: ' + str(self.parameters))
        datafile_path = self.parameters['target_path'] + f'/data/{self.parameters["benchmark"]}_{self.parameters["max_index_width"]}_{self.parameters["max_indexes_per_query"]}-{convert_dict_to_filename_component(self.parameters["extra_algorithms"])}.dat'
        if os.path.isfile(datafile_path) and not self.parameters['overwrite']:
            logging.info(f'Datafile already exists in path {datafile_path}. If you want to overwrite it, set overwrite parameter to true.')
            return []

        heuristic_indexes = self.extra_algorithms(workload)

        time_start = time.time()
        COSTS_PER_QUERY_WITHOUT_INDEXES = {}
        for query in workload.queries:
            COSTS_PER_QUERY_WITHOUT_INDEXES[query] = self.cost_evaluation.calculate_cost(Workload([query]), set())

        accessed_columns_per_table = {}
        for query in workload.queries:
            for column in query.columns:
                if column.table not in accessed_columns_per_table:
                    accessed_columns_per_table[column.table] = set()
                accessed_columns_per_table[column.table].add(column)

        candidate_indexes = set()
        for number_of_index_columns in range(1, self.parameters['max_index_width'] + 1):
            for table in accessed_columns_per_table:
                for index_columns in itertools.permutations(accessed_columns_per_table[table], number_of_index_columns):
                    candidate_indexes.add(Index(index_columns))

        # stores indexes that have a benefit in any combination (to prune indexes with no benefit)
        useful_indexes = set().union(heuristic_indexes)
        costs_for_index_combination = {}

        for number_of_indexes_per_query in range(1, self.parameters['max_indexes_per_query'] + 1):
            for index_combination in itertools.combinations(candidate_indexes, number_of_indexes_per_query):
                is_useful_combination = False
                costs = []
                for query in workload.queries:
                    query_cost = self.cost_evaluation.calculate_cost(Workload([query]), set(index_combination), store_size=True)
                    costs.append(query_cost)
                    if query_cost < COSTS_PER_QUERY_WITHOUT_INDEXES[query]:
                        is_useful_combination = True
                if is_useful_combination:
                    costs_for_index_combination[index_combination] = costs
                    for index in index_combination:
                        useful_indexes.add(index)
                        #print(f'index {index}')

        os.makedirs(f"{self.parameters['target_path']}/data", exist_ok=True)
        with open(datafile_path, 'w+') as file:
            file.write(f'#{len(useful_indexes)}\n')
            file.write(f'#{len(costs_for_index_combination)}\n')
            file.write(f'# what-if time: {time.time() - time_start}\n')
            file.write(f'# cost_requests: {self.cost_evaluation.cost_requests}\tcache_hits: {self.cost_evaluation.cache_hits}\n')
            # generate AMPL input
            # sorted_useful_indexes = sorted(useful_indexes)
            file.write('data;\n\n')
            # file.write(f'set INDEXES := 1..{len(useful_indexes)};\n')
            # file.write(f'set COMBINATIONS := 0..{len(costs_for_index_combination)};\n')
            #file.write('set QUERIES := ')
            #for num in workload.queries:
            #    file.write(f' {num.nr}')
            #file.write(';\n')

            # print size of index and determine index_ids, which are used in combinations
            index_ids = {}
            file.write('\nparam a :=\n')
            for i, index in enumerate(sorted(useful_indexes)):
                assert(index.estimated_size), "Index size must be set."
                file.write(f"{i + 1} {index.estimated_size} # {index._column_names()}\n")
                index_ids[index] = i + 1
            file.write(';\n\n')

            # print index_ids per combination
            # combi 0 := no index
            file.write(f"set combi[0]:= ;\n")
            for i, index_combination in enumerate(costs_for_index_combination):
                index_id_list = [str(index_ids[index]) for index in index_combination]
                file.write(f"set combi[{i + 1}]:= {' '.join(index_id_list)};\n")

            # print costs per query and index_combination
            file.write('\nparam f4 :=\n')
            for query_id, query in enumerate(workload.queries):
                # Print cost without indexes
                file.write(f'{query_id + 1} 0 {COSTS_PER_QUERY_WITHOUT_INDEXES[query]}\n')
                for i, index_combination in enumerate(costs_for_index_combination):
                    costs = costs_for_index_combination[index_combination]
                    # print cost if they are lower than without indexes
                    if costs[query_id] < COSTS_PER_QUERY_WITHOUT_INDEXES[query]:
                        file.write(f'{query_id + 1} {i + 1} {costs[query_id]}\n')
            file.write(';\n')

        return []

    def extra_algorithms(self, workload: Workload) -> Set[Index]:

        algorithms: Dict[str, SelectionAlgorithm] = {
            "anytime": AnytimeAlgorithm,
            "auto_admin": AutoAdminAlgorithm,
            "db2advis": DB2AdvisAlgorithm,
            "dexter": DexterAlgorithm,
            "drop": DropHeuristicAlgorithm,
            "extend": ExtendAlgorithm,
            "relaxation": RelaxationAlgorithm,
        }

        heuristic_indexes = set()

        for extra_algorithm in self.parameters['extra_algorithms'].keys():
            if extra_algorithm not in algorithms.keys():
                logging.error(f'{extra_algorithm} is NOT a valid extension algorithm')
                continue
            logging.info(f'---running {extra_algorithm} from cophy---')
            algorithm = algorithms[extra_algorithm](self.database_connector, parameters=self.parameters['extra_algorithms'][extra_algorithm])
            algorithm.calculate_best_indexes(workload)
            logging.info(f'---Completed {extra_algorithm} from cophy---')
            heuristic_indexes = heuristic_indexes.union(algorithm.result_indexes)
        return heuristic_indexes

def convert_dict_to_filename_component(dict: Dict[str, Dict[str, Any]]) -> str:
    names = dict.keys()
    names = sorted(names)
    return '-'.join(names)