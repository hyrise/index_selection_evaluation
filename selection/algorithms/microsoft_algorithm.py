from ..selection_algorithm import SelectionAlgorithm
from .microsoft.candidate_index_selection import CandidateIndexSelection
from .microsoft.configuration_enumeration import ConfigurationEnumeration
from .microsoft.multi_column_index_generation import MultiColumnIndexGeneration
import logging


# multi column index methods: 'no', 'lead', 'all'
# cost_estimation: 'whatif' or 'acutal_runtimes'
DEFAULT_PARAMETERS = {'max_indexes': 15, 'max_indexes_naive': 3,
                      'max_index_columns': 2, 'cost_estimation': 'whatif',
                      'pruning': True}


class MicrosoftAlgorithm(SelectionAlgorithm):
    def __init__(self, database_connector, parameters):
        SelectionAlgorithm.__init__(self, database_connector, parameters,
                                    DEFAULT_PARAMETERS)
        max_indexes = self.parameters['max_indexes']
        max_indexes_naive = self.parameters['max_indexes_naive']
        if max_indexes < max_indexes_naive:
            self.parameters['max_indexes_naive'] = max_indexes

    def _calculate_best_indexes(self, workload):
        logging.info('Calculating best indexes (microsoft)')
        logging.info('Parameters: ' + str(self.parameters))

        if self.parameters['max_indexes'] == 0:
            return []
        evaluation_method = self.parameters['cost_estimation']
        self.cost_evaluation.cost_estimation = evaluation_method

        potential_indexes = self.potential_indexes(workload)
        candidate_selection = CandidateIndexSelection(self.cost_evaluation,
                                                      self)
        enumeration = ConfigurationEnumeration([], workload,
                                               self.cost_evaluation, self)
        multicolumn_indexes = MultiColumnIndexGeneration(potential_indexes)

        for i in range(self.parameters['max_index_columns']):
            self.cost_evaluation.cost_estimation = 'whatif'
            candidates = candidate_selection.select(workload,
                                                    potential_indexes)
            self.cost_evaluation.cost_estimation = evaluation_method
            enumeration.candidate_indexes = candidates
            enumeration.lowest_cost = None
            indexes = enumeration.enumerate()
            if i < self.parameters['max_index_columns'] - 1:
                potential_indexes = multicolumn_indexes.create(indexes)
                potential_indexes.extend(indexes)
        return indexes
