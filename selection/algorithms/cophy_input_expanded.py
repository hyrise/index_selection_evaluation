import os
from typing import Any, Dict, Set
from selection.algorithms.anytime_algorithm import AnytimeAlgorithm
from selection.algorithms.auto_admin_algorithm import AutoAdminAlgorithm
from selection.algorithms.cophy_input_generation import CoPhyInputGeneration
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
DEFAULT_PARAMETERS = {
    "max_index_width": 2,
    "max_indexes_per_query": 1,
    "benchmark_name": "benchmark",
    "json_path": "cophy_json",
    "file_path": "cophy_data",
    "overwrite": False,
    "extra_algorithms": {"None":{}}
}
class CoPhyExpandedAlgorithm(SelectionAlgorithm):
    def __init__(self, database_connector, parameters=None):
        if parameters is None:
            parameters = {}
        SelectionAlgorithm.__init__(self, database_connector, parameters,
                                    DEFAULT_PARAMETERS)

    def _calculate_best_indexes(self, workload: Workload):
        logging.info('Creating AMPL input for CoPhy')
        logging.info('Parameters: ' + str(self.parameters))

        if self.parameters["file_path"]:
            datafile_path = (
                self.parameters["file_path"]
                + f'/{self.parameters["benchmark_name"] }_{self.parameters["max_index_width"]}_{self.parameters["max_indexes_per_query"]}-{convert_dict_to_filename_component(self.parameters["extra_algorithms"])}.dat'
            )
            if os.path.isfile(datafile_path) and not self.parameters["overwrite"]:
                logging.info(
                    f"A datafile already exists for at {datafile_path}. Set parameter overwrite to True if you want to overwrite. Aborting Run"
                )
                return []

        # This is a bit clumsy as no dat file can be generated if a JSON already exists. However a tool to convert the json into a dat is somewhat trivial
        if self.parameters["json_path"]:
            json_file_path = (
                self.parameters["json_path"]
                + f'/{self.parameters["benchmark_name"]}_{self.parameters["max_index_width"]}_{self.parameters["max_indexes_per_query"]}-{convert_dict_to_filename_component(self.parameters["extra_algorithms"])}.json'
            )
            if os.path.isfile(datafile_path) and not self.parameters["overwrite"]:
                logging.info(
                    f"A jsonfile already exists for at {json_file_path}. Set parameter overwrite to True if you want to overwrite. Aborting Run"
                )
                return []

        if not self.parameters["file_path"] and not self.parameters["json_path"]:
            logging.warning(
                "No output files are being generated. The algorithm will run but no output will be available."
            )

        if self.parameters["benchmark_name"] == "benchmark":
            logging.info(
                'It is recommended to set the "benchmark_name" parameter to remember which benchmark this algorithm was executed on.'
            )

        time_start = time.time()

        heuristic_indexes = self.extra_algorithms(workload)

        COSTS_PER_QUERY_WITHOUT_INDEXES = {}
        for query in workload.queries:
            COSTS_PER_QUERY_WITHOUT_INDEXES[
                query
            ] = self.cost_evaluation.calculate_cost(Workload([query]), set())

        accessed_columns_per_table = {}
        for query in workload.queries:
            for column in query.columns:
                if column.table not in accessed_columns_per_table:
                    accessed_columns_per_table[column.table] = set()
                accessed_columns_per_table[column.table].add(column)

        candidate_indexes = set()
        for number_of_index_columns in range(1, self.parameters["max_index_width"] + 1):
            for table in accessed_columns_per_table:
                for index_columns in itertools.permutations(
                    accessed_columns_per_table[table], number_of_index_columns
                ):
                    candidate_indexes.add(Index(index_columns))

        # stores indexes that have a benefit in any combination (to prune indexes with no benefit)
        useful_indexes: Set[Index] = set()
        costs_for_index_combination = {}

        for number_of_indexes_per_query in range(
            1, self.parameters["max_indexes_per_query"] + 1
        ):
            for index_combination in itertools.combinations(
                candidate_indexes, number_of_indexes_per_query
            ):
                is_useful_combination = False
                costs_per_query = {}
                for query in workload.queries:
                    query_cost = self.cost_evaluation.calculate_cost(
                        Workload([query]), set(index_combination), store_size=True
                    )
                    # test if query_cost is lower than default cost
                    if query_cost < COSTS_PER_QUERY_WITHOUT_INDEXES[query]:
                        is_useful_combination = True
                        costs_per_query[query] = query_cost
                if is_useful_combination:
                    costs_for_index_combination[index_combination] = costs_per_query
                    for index in index_combination:
                        useful_indexes.add(index)

        cophy_dict = {}
        cophy_dict["what_if_time"] = time.time() - time_start
        cophy_dict["cost_requests"] = self.cost_evaluation.cost_requests
        cophy_dict["cache_hits"] = self.cost_evaluation.cache_hits
        cophy_dict["num_useful_indexes"] = len(useful_indexes)
        cophy_dict["num_combination_costs"] = len(costs_for_index_combination)
        # generate AMPL input
        # sorted_useful_indexes = sorted(useful_indexes)
        cophy_dict["queries"] = []
        for query in workload.queries:
            cophy_dict["queries"].append(query.nr)

        # print size of index and determine index_ids, which are used in combinations
        index_ids = {}
        cophy_dict["index_costs"] = []
        for i, index in enumerate(sorted(useful_indexes)):
            assert index.estimated_size, "Index size must be set."
            cophy_dict["index_costs"].append(
                {
                    "id": i + 1,
                    "estimated_size": index.estimated_size,
                    "column_names": index._column_names(),
                }
            )
            index_ids[index] = i + 1

        # print index_ids per combination
        # combi 0 := no index
        cophy_dict["combi"] = []
        for i, index_combination in enumerate(costs_for_index_combination):
            index_id_list = [str(index_ids[index]) for index in index_combination]
            cophy_dict["combi"].append(
                {"combi_id": i + 1, "index_ids": " ".join(index_id_list)}
            )

        # print costs per query and index_combination
        cophy_dict["f4"] = []
        for query in workload.queries:
            # Print cost without indexes
            cophy_dict["f4"].append(
                {
                    "query_number": query.nr,
                    "combi_number": 0,
                    "costs": COSTS_PER_QUERY_WITHOUT_INDEXES[query],
                }
            )
            for i, index_combination in enumerate(costs_for_index_combination):
                # query is in dictionary if cost is lower than default
                if query in costs_for_index_combination[index_combination]:
                    cophy_dict["f4"].append(
                        {
                            "query_number": query.nr,
                            "combi_number": i + 1,
                            "costs": costs_for_index_combination[index_combination][
                                query
                            ],
                        }
                    )

       # if self.parameters["json_path"]:
       #     save_as_json(self.parameters["json_path"], json_file_path, cophy_dict)

       # if self.parameters["file_path"]:
       #     save_cophy_as_file(self.parameters["file_path"], datafile_path, cophy_dict)

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