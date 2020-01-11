from ..selection_algorithm import SelectionAlgorithm
import logging
import itertools


DEFAULT_PARAMETERS = {'max_indexes': 15, 'pruning': True,
                      'cost_estimation': 'whatif'}


class IndexDropping:
    def __init__(self, all_indexes, workload, cost_evaluation, parameters):
        self.indexes = all_indexes
        self.workload = workload
        self.cost_evaluation = cost_evaluation
        self.parameters = parameters
        self._drop_indexes()

    def _cost(self, indexes):
        cost = self.cost_evaluation.calculate_cost(self.workload, indexes)
        return cost

    def _drop_indexes(self, number_dropped_indexes=1):
        logging.debug('Dopping, indexes size: {}'.format(len(self.indexes)))
        # cost_before_dropping = self._cost(self.indexes)
        index_combinations = itertools.combinations(self.indexes,
                                                    number_dropped_indexes)
        lowest_cost = None
        for index_combination in index_combinations:
            indexes = self._remove_from_index_set(index_combination)
            cost = self._cost(indexes)
            if not lowest_cost or cost < lowest_cost[0]:
                lowest_cost = (cost, indexes)
        self.indexes = lowest_cost[1]
        if len(self.indexes) > self.parameters['max_indexes']:
            self._drop_indexes()

    def _remove_from_index_set(self, index_combination):
        return [x for x in self.indexes if x not in index_combination]


class DropHeuristicAlgorithm(SelectionAlgorithm):
    def __init__(self, database_connector, parameters={}):
        SelectionAlgorithm.__init__(self, database_connector, parameters,
                                    DEFAULT_PARAMETERS)

    def calculate_best_indexes(self, workload):
        logging.info('Calculating best indexes (drop heuristic)')
        logging.info('Parameters: ' + str(self.parameters))
        if self.parameters['pruning']:
            self.cost_evaluation.reset_pruning()
            logging.info('Use pruning')

        all_indexes = self.potential_indexes(workload)
        # index_dropping = IndexDropping(all_indexes, workload,
        #                                self.cost_evaluation, self.parameters)
        # return index_dropping.indexes

        evaluation_method = self.parameters['cost_estimation']
        self.cost_evaluation.cost_estimation = evaluation_method

        simulate = False
        what_if = self.cost_evaluation.what_if
        if self.cost_evaluation.cost_estimation == 'actual_runtimes':
            self.cost_evaluation.create_all_indexes(all_indexes)
            simulate = True
        elif self.cost_evaluation.cost_estimation == 'whatif':
            for index in all_indexes:
                what_if.simulate_index(index, store_size=True)
        else:
            raise Exception('only whatif or actual_runtimes')
        # initial_cost = self.cost_evaluation.calculate_cost(workload,
        #                                                    all_indexes)
        # print('ini', initial_cost)
        while True:
            logging.debug(f'len indexes {len(all_indexes)}')
            if self.parameters['pruning']:
                hits = self.cost_evaluation.pruning_hits
                logging.debug(f'pruning hits {hits[0]}, calls {hits[1]}')
            indexes = all_indexes.copy()
            best = None
            for index in all_indexes:
                indexes.remove(index)
                if self.cost_evaluation.cost_estimation == 'whatif':
                    what_if.drop_simulated_index(index)
                cost = self.cost_evaluation.calculate_cost(workload, indexes,
                                                           store_size=True,
                                                           simulate=simulate)
                if not best or cost < best[1]:
                    best = [index, cost]
                indexes.append(index)
                if self.cost_evaluation.cost_estimation == 'whatif':
                    what_if.simulate_index(index)
            all_indexes.remove(best[0])
            if self.cost_evaluation.cost_estimation == 'whatif':
                what_if.drop_simulated_index(best[0])

            if len(all_indexes) <= self.parameters['max_indexes']:
                break
        if self.cost_evaluation.cost_estimation == 'actual_runtimes':
            self.cost_evaluation.db_connector.drop_indexes()
        elif self.cost_evaluation.cost_estimation == 'whatif':
            what_if.drop_all_simulated_indexes()
        return all_indexes
