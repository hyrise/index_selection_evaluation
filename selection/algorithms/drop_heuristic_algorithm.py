from ..selection_algorithm import SelectionAlgorithm
import logging
import itertools


# Algorithm stops when maximum number of indexes is reached
DEFAULT_PARAMETERS = {
    'max_indexes': 15,
    'cost_estimation': 'whatif'
}


class DropHeuristicAlgorithm(SelectionAlgorithm):
    def __init__(self, database_connector, parameters={}):
        SelectionAlgorithm.__init__(self, database_connector, parameters,
                                    DEFAULT_PARAMETERS)

    def _calculate_best_indexes(self, workload):
        logging.info('Calculating best indexes (drop heuristic)')
        logging.info('Parameters: ' + str(self.parameters))

        # remaining_indexes is initialized as set of all potential indexes
        remaining_indexes = set(self.potential_indexes(workload))

        while len(remaining_indexes) > self.parameters['max_indexes']:
            # drop one index (with the lowest cost) per iteration
            lowest_cost = None
            index_to_drop = None
            for index in remaining_indexes:
                cost = self.cost_evaluation.calculate_cost(workload, remaining_indexes - set([index]))
                if not lowest_cost or cost < lowest_cost:
                    lowest_cost, index_to_drop = cost, index
            old_len = len(remaining_indexes)
            remaining_indexes.remove(index_to_drop)
            assert old_len - 1 == len(remaining_indexes)

        return remaining_indexes
