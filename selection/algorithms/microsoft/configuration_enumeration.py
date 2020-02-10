import itertools
import logging


class ConfigurationEnumeration():
    def __init__(self,
                 candidate_indexes,
                 workload,
                 cost_evaluation,
                 microsoft_algorithm,
                 candidate_selection=False):
        parameters = microsoft_algorithm.parameters
        self.max_indexes_naive = parameters['max_indexes_naive']
        self.max_indexes = parameters['max_indexes']
        self.candidate_indexes = candidate_indexes
        self.workload = workload
        self.cost_evaluation = cost_evaluation

        self.candidate_selection = candidate_selection

        self.indexes = None
        self.lowest_cost = None

    def enumerate(self):
        # Set a lower logging level to reduce debug output during
        # candidate selection.
        # Numeric value of DEBUG level is 10
        level = 5 if self.candidate_selection else 10

        log_out = 'Start Enumeration\n\tNumber of candidate indexes: '
        log_out += str(len(self.candidate_indexes))
        log_out += '\n\tNumber of indexes to be selected:'
        log_out += str(self.max_indexes)
        logging.log(level, log_out)

        # Make sure to not evaluate indexes multiple times
        number_indexes_naive = min(self.max_indexes_naive,
                                   len(self.candidate_indexes))
        self.enumerate_naive(number_indexes_naive)
        log_out = 'lowest cost (naive): {}\n\t'.format(self.lowest_cost)
        log_out += 'lowest cost indexes (naive): {}'.format(self.indexes)
        logging.log(level, log_out)

        number_indexes = min(self.max_indexes, len(self.candidate_indexes))
        self.enumerate_greedy(number_indexes, level)
        log_out = 'lowest cost (greedy): {}\n\t'.format(self.lowest_cost)
        log_out += 'lowest cost indexes (greedy): {}\n'.format(self.indexes)
        log_out += '(greedy): number indexes {}\n'.format(len(self.indexes))
        logging.log(level, log_out)

        return self.indexes

    def enumerate_greedy(self, number_indexes, level):
        if len(self.indexes) >= number_indexes:
            return
        # (index, cost)
        best_index = (None, None)
        index_set = [
            item for item in self.candidate_indexes if item not in self.indexes
        ]
        logging.log(level, 'Searching in {} columns'.format(len(index_set)))

        for index in index_set:
            cost = self._simulate_and_evaluate_cost(self.indexes + [index])
            if not best_index[0] or cost < best_index[1]:
                best_index = (index, cost)
        if best_index[0] and best_index[1] < self.lowest_cost:
            self.indexes.append(best_index[0])
            self.lowest_cost = best_index[1]
            log_out = 'Additional best index found: {}'.format(best_index)
            logging.log(level, log_out)
            self.enumerate_greedy(number_indexes, level)

    def enumerate_naive(self, number_indexes_naive):
        lowest_cost_indexes = None

        for number_indexes in range(number_indexes_naive + 1):
            index_combis = itertools.combinations(self.candidate_indexes,
                                                  number_indexes)
            for index_combi in index_combis:
                cost = self._simulate_and_evaluate_cost(index_combi)
                if not self.lowest_cost or cost < self.lowest_cost:
                    self.lowest_cost = cost
                    lowest_cost_indexes = index_combi
        self.indexes = list(lowest_cost_indexes)

    def _simulate_and_evaluate_cost(self, indexes):
        cost = self.cost_evaluation.calculate_cost(self.workload,
                                                   indexes,
                                                   store_size=True)
        return round(cost, 2)
