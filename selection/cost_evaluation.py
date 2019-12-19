from .what_if_index_creation import WhatIfIndexCreation
import logging


class CostEvaluation():
    def __init__(self, db_connector, cost_estimation='whatif'):
        logging.debug('Init cost evaluation')
        self.db_connector = db_connector
        self.cost_estimation = cost_estimation
        logging.info('Cost estimation with ' + self.cost_estimation)
        self.what_if = WhatIfIndexCreation(db_connector)
        self.pruning = False
        self.pruning_hits = [0, 0]
        self.costs = {}

    def reset_pruning(self):
        self.pruning = True
        self.pruning_hits = [0, 0]
        self.costs = {}

    def calculate_cost(self, workload, indexes, store_size=False,
                       simulate=True):
        if simulate:
            self._prepare_cost_calculation(indexes, store_size=store_size)
        total_cost = 0

        # TODO: Make query cost higher for queries which are running often

        # TODO refactor costs cache
        for query in workload.queries:
            cost = None
            if self.pruning:
                relevant_indexes = [x for x in indexes
                                    if any(c in query.columns
                                           for c in x.columns)]
                result = False
                relevant_indexes = sorted(relevant_indexes)
                if query in self.costs:
                    list = self.costs[query]
                    result = next((x for x in list
                                   if x[1] == relevant_indexes), False)
                    if result:
                        self.pruning_hits[0] += 1
                        cost = result[0]
            if not cost:
                cost = self._request_total_cost(query)

                if self.pruning:
                    self.pruning_hits[1] += 1
                    if query not in self.costs:
                        self.costs[query] = []
                    if not result:
                        self.costs[query].append((cost, relevant_indexes))

            total_cost += cost
        if simulate:
            self._complete_cost_estimation()
        return total_cost

    def _request_total_cost(self, query):
        total_cost = self.db_connector.get_cost(query)
        return total_cost

    def _prepare_cost_calculation(self, indexes, store_size=False):
        if self.cost_estimation == 'whatif':
            for index in indexes:
                self.what_if.simulate_index(index, store_size=store_size)

    def _complete_cost_estimation(self):
        self.what_if.drop_all_simulated_indexes()
