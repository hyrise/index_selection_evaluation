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
        self.current_indexes = set()
        # [cache hits, database cost requests]
        self.pruning_hits = [0, 0]
        self.cache = {}

    def calculate_cost(self, workload, indexes, store_size=False):
        self._prepare_cost_calculation(indexes, store_size=store_size)
        total_cost = 0

        # TODO: Make query cost higher for queries which are running often
        for query in workload.queries:
            total_cost += self._request_cache(query, indexes)
        return total_cost

    # Creates the current index combination by simulating/creating
    # missing indexes and unsimulating/dropping indexes
    # that exist but are not in the combination.
    def _prepare_cost_calculation(self, indexes, store_size=False):
        drop_indexes = self.current_indexes.copy()

        for index in indexes:
            if index not in self.current_indexes:
                self._simulate_or_create_index(index, store_size)
            else:
                drop_indexes.remove(index)
        for drop_index in drop_indexes:
            self._unsimulate_or_drop_index(drop_index)

        self.current_indexes = set(indexes)

    def _simulate_or_create_index(self, index, store_size):
        if self.cost_estimation == 'whatif':
            self.what_if.simulate_index(index, store_size=store_size)
        elif self.cost_estimation == 'actual_runtimes':
            # TODO
            pass

    def _unsimulate_or_drop_index(self, index):
        if self.cost_estimation == 'whatif':
            self.what_if.drop_simulated_index(index)
        elif self.cost_estimation == 'actual_runtimes':
            # TODO
            pass

    def _get_cost(self, query):
        if self.cost_estimation == 'whatif':
            return self.db_connector.get_cost(query)
        elif self.cost_estimation == 'actual_runtimes':
            # TODO
            return 0

    def complete_cost_estimation(self):
        for index in self.current_indexes:
            self._unsimulate_or_drop_index(index)

    def _request_cache(self, query, indexes):
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
            cost = self._get_cost(query)
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
