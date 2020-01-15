from .cost_evaluation import CostEvaluation
import logging


class SelectionAlgorithm:
    def __init__(self, database_connector, parameters, default_parameters={}):
        logging.debug('Init selection algorithm')
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
        self.cost_evaluation.reset()
        indexes = self._calculate_best_indexes(workload)
        hits = self.cost_evaluation.pruning_hits
        logging.debug(f'pruning hits {hits[0]}, calls {hits[1]}')
        self.cost_evaluation.complete_cost_estimation()
        return indexes

    def _calculate_best_indexes(self, workload):
        raise NotImplementedError('_calculate_best_indexes(self, '
                                  'workload) missing')

    def indexable_columns(self, workload):
        return workload.indexable_columns()

    def potential_indexes(self, workload):
        return [c.single_column_index for c
                in self.indexable_columns(workload)]


class NoIndexAlgorithm(SelectionAlgorithm):
    def __init__(self, database_connector, parameters={}):
        SelectionAlgorithm.__init__(self, database_connector, parameters)

    def calculate_best_indexes(self, workload):
        return []


class AllIndexesAlgorithm(SelectionAlgorithm):
    def __init__(self, database_connector, parameters={}):
        SelectionAlgorithm.__init__(self, database_connector, parameters)

    # Returns single column index for each indexable column
    def calculate_best_indexes(self, workload):
        return self.potential_indexes(workload)
