from .what_if_index_creation import WhatIfIndexCreation
import logging


class CostEvaluation():
    def __init__(self, db_connector, cost_estimation='whatif'):
        logging.debug('Init cost evaluation')
        self.db_connector = db_connector
        self.cost_estimation = cost_estimation
        logging.info('Cost estimation with ' + self.cost_estimation)
        self.what_if = WhatIfIndexCreation(db_connector)
        self.reset()

    def reset(self):
        self.current_indexes = []
        # [cache hits, database cost requests]
        self.pruning_hits = [0, 0]
        self.cache = {}

    def calculate_cost(self, workload, indexes, store_size=False):
        self._prepare_cost_calculation(indexes, store_size=store_size)
        total_cost = 0

        # TODO: Make query cost higher for queries which are running often
        for query in workload.queries:
            total_cost += self.request_cache(query, indexes,
                                             self.db_connector)
        self._complete_cost_estimation()
        return total_cost

    def _prepare_cost_calculation(self, indexes, store_size=False):
        if self.cost_estimation == 'whatif':
            for index in indexes:
                self.what_if.simulate_index(index, store_size=store_size)

    def _complete_cost_estimation(self):
        self.what_if.drop_all_simulated_indexes()

    def request_cache(self, query, indexes, db_connector):
        cost = None
        relevant_indexes = self._relevant_indexes(query, indexes)

        # Check if query and corresponding relevant indexes in cache
        if query in self.cache:
            result = next((x for x in self.cache[query]
                           if x[1] == relevant_indexes), False)
            if result:
                self.pruning_hits[0] += 1
                cost = result[0]
        # If no cache hit request cost from database system
        if not cost:
            cost = db_connector.get_cost(query)
            self.pruning_hits[1] += 1
            if query not in self.cache:
                self.cache[query] = []
            self.cache[query].append((cost, relevant_indexes))
        return cost

    def _relevant_indexes(self, query, indexes):
        relevant_indexes = [x for x in indexes
                            if any(c in query.columns
                                   for c in x.columns)]
        relevant_indexes = sorted(relevant_indexes)
        return relevant_indexes
