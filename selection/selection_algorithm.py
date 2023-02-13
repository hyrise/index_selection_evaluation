import logging
from typing import Optional, Set

from selection.index import Index
from selection.workload import Workload

from .cost_evaluation import CostEvaluation

# If not specified by the user, algorithms should use these default parameter values to
# avoid diverging values for different algorithms.
DEFAULT_PARAMETER_VALUES = {
    "budget_MB": 500,
    "max_indexes": 15,
    "max_index_width": 2,
}


class SelectionAlgorithm:

    result_indexes: Optional[Set[Index]]

    def __init__(
        self,
        database_connector,
        parameters,
        global_config,
        name,
        default_parameters=None,
    ):
        if default_parameters is None:
            default_parameters = {}
        logging.debug("Init selection algorithm")
        self.did_run = False
        self.parameters = parameters
        # Store default values for missing parameters
        for key, value in default_parameters.items():
            if key not in self.parameters:
                self.parameters[key] = value

        self.database_connector = database_connector
        # TODO Is this superflous usually?
        self.database_connector.drop_indexes()
        self.cost_evaluation = CostEvaluation(database_connector)
        if "cost_estimation" in self.parameters:
            estimation = self.parameters["cost_estimation"]
            self.cost_evaluation.cost_estimation = estimation

        self.global_config = global_config
        self.name = name

    def calculate_best_indexes(self, workload: Workload):
        assert self.did_run is False, "Selection algorithm can only run once."
        self.did_run = True
        indexes = self._calculate_best_indexes(workload)
        self._log_cache_hits()
        self.cost_evaluation.complete_cost_estimation()

        if 'dump_cache' in self.global_config and self.global_config['dump_cache']:
            self._dump_cache()
        return indexes

    def _calculate_best_indexes(self, workload):
        raise NotImplementedError("_calculate_best_indexes(self, workload) missing")

    def _log_cache_hits(self):
        hits = self.cost_evaluation.cache_hits
        requests = self.cost_evaluation.cost_requests
        logging.debug(f"Total cost cache hits:\t{hits}")
        logging.debug(f"Total cost requests:\t\t{requests}")
        if requests == 0:
            return
        ratio = round(hits * 100 / requests, 2)
        logging.debug(f"Cost cache hit ratio:\t{ratio}%")

    def _dump_cache(self) -> None:
        print(f'----------\n{self.parameters}\n{self.global_config}')
        path = (
            f'cache-{self.global_config["benchmark_name"]}-{self.name}-'
            + f'{self.parameters["max_index_width"]}-{self.parameters["budget_MB"]}.json'
        )
        self.cost_evaluation.dump_cache(path)


class NoIndexAlgorithm(SelectionAlgorithm):
    def __init__(self, database_connector, global_config, name, parameters=None):
        if parameters is None:
            parameters = {}
        SelectionAlgorithm.__init__(self, database_connector, parameters, name, global_config)

    def _calculate_best_indexes(self, workload):
        return []


class AllIndexesAlgorithm(SelectionAlgorithm):
    def __init__(self, database_connector, global_config, name, parameters=None):
        if parameters is None:
            parameters = {}
        SelectionAlgorithm.__init__(self, database_connector, global_config, name, parameters)

    # Returns single column index for each indexable column
    def _calculate_best_indexes(self, workload):
        return workload.potential_indexes()
