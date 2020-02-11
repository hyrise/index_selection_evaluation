from .cost_evaluation import CostEvaluation
from .index import Index
import logging


class SelectionAlgorithm:
    def __init__(self, database_connector, parameters, default_parameters={}):
        logging.debug('Init selection algorithm')
        self.did_run = False
        self.parameters = parameters
        if not parameters:
            self.parameters = {}
        # Store default values for missing parameters
        for key, value in default_parameters.items():
            if key not in self.parameters:
                self.parameters[key] = value

        self.database_connector = database_connector
        self.database_connector.drop_indexes()
        self.cost_evaluation = CostEvaluation(database_connector)
        if 'cost_estimation' in self.parameters:
            estimation = self.parameters['cost_estimation']
            self.cost_evaluation.cost_estimation = estimation

    def calculate_best_indexes(self, workload):
        assert self.did_run == False, 'Selection algorithm can only run once.'

        self.did_run = True
        indexes = self._calculate_best_indexes(workload)
        self._log_cache_hits()
        self.cost_evaluation.complete_cost_estimation()

        return indexes

    def _calculate_best_indexes(self, workload):
        raise NotImplementedError('_calculate_best_indexes(self, '
                                  'workload) missing')

    def _log_cache_hits(self):
        hits = self.cost_evaluation.cache_hits
        requests = self.cost_evaluation.cost_requests
        logging.debug(f'Total cost cache hits:\t{hits}')
        logging.debug(f'Total cost requests:\t\t{requests}')
        if requests == 0:
            return
        ratio = round(hits * 100 / requests, 2)
        logging.debug(f'Cost cache hit ratio:\t{ratio}%')

    def indexable_columns(self, workload):
        return workload.indexable_columns()

    def potential_indexes(self, workload):
        return [Index([c]) for c in self.indexable_columns(workload)]


class NoIndexAlgorithm(SelectionAlgorithm):
    def __init__(self, database_connector, parameters={}):
        SelectionAlgorithm.__init__(self, database_connector, parameters)

    def _calculate_best_indexes(self, workload):
        return []


class AllIndexesAlgorithm(SelectionAlgorithm):
    def __init__(self, database_connector, parameters={}):
        SelectionAlgorithm.__init__(self, database_connector, parameters)

    # Returns single column index for each indexable column
    def _calculate_best_indexes(self, workload):
        return self.potential_indexes(workload)
