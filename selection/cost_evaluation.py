from .what_if_index_creation import WhatIfIndexCreation
import logging


class CostEvaluation:
    def __init__(self, db_connector, cost_estimation='whatif'):
        logging.debug('Init cost evaluation')
        self.db_connector = db_connector
        self.cost_estimation = cost_estimation
        logging.info('Cost estimation with ' + self.cost_estimation)
        self.what_if = WhatIfIndexCreation(db_connector)
        self.current_indexes = set()
        self.cost_requests = 0
        self.cache_hits = 0
        # Cache structure:
        # {(query_object, relevant_indexes): cost}
        self.cache = {}
        self.completed = False
        # It is not necessary to drop hypothetical indexes during __init__().
        # These are only created per connection. Hence, non should be present.

    def calculate_cost(self, workload, indexes, store_size=False):
        assert self.completed == False, 'Cost Evaluation is completed and cannot be reused.'
        self._prepare_cost_calculation(indexes, store_size=store_size)
        total_cost = 0

        # TODO: Make query cost higher for queries which are running often
        for query in workload.queries:
            self.cost_requests += 1
            total_cost += self._request_cache(query, indexes)
        return total_cost

    # Creates the current index combination by simulating/creating
    # missing indexes and unsimulating/dropping indexes
    # that exist but are not in the combination.
    def _prepare_cost_calculation(self, indexes, store_size=False):
        for index in set(indexes) - self.current_indexes:
            self._simulate_or_create_index(index, store_size=store_size)
        for index in self.current_indexes - set(indexes):
            self._unsimulate_or_drop_index(index)

        self.current_indexes = set(indexes)

    def _simulate_or_create_index(self, index, store_size=False):
        if self.cost_estimation == 'whatif':
            self.what_if.simulate_index(index, store_size=store_size)
        elif self.cost_estimation == 'actual_runtimes':
            self.db_connector.create_index(index)

    def _unsimulate_or_drop_index(self, index):
        if self.cost_estimation == 'whatif':
            self.what_if.drop_simulated_index(index)
        elif self.cost_estimation == 'actual_runtimes':
            self.db_connector.drop_index(index)

    def _get_cost(self, query):
        if self.cost_estimation == 'whatif':
            return self.db_connector.get_cost(query)
        elif self.cost_estimation == 'actual_runtimes':
            runtime = self.db_connector.exec_query(query)[0]
            return runtime

    def complete_cost_estimation(self):
        self.completed = True

        for index in self.current_indexes:
            self._unsimulate_or_drop_index(index)

        self.current_indexes = set()

    def _request_cache(self, query, indexes):
        relevant_indexes = self._relevant_indexes(query, indexes)

        # Check if query and corresponding relevant indexes in cache
        if (query, relevant_indexes) in self.cache:
            self.cache_hits += 1
            return self.cache[(query, relevant_indexes)]
        # If no cache hit request cost from database system
        else:
            cost = self._get_cost(query)
            self.cache[(query, relevant_indexes)] = cost
            return cost

    def _relevant_indexes(self, query, indexes):
        relevant_indexes = [
            x for x in indexes if any(c in query.columns for c in x.columns)
        ]
        return frozenset(relevant_indexes)
