from ..selection_algorithm import SelectionAlgorithm
from ..what_if_index_creation import WhatIfIndexCreation
from ..index import Index
import logging
import time
import random


# Maxiumum number of columns per index, storage budget in MB,
# time to "try variations" in seconds (see IBM paper),
# maximum index candidates removed while try_variations
DEFAULT_PARAMETERS = {
    'max_index_columns': 3,
    'budget': 500,
    'try_variation_seconds_limit': 10,
    'try_variation_maximum_remove': 4
}


class IBMAlgorithm(SelectionAlgorithm):
    def __init__(self, database_connector, parameters):
        SelectionAlgorithm.__init__(self, database_connector, parameters,
                                    DEFAULT_PARAMETERS)
        self.what_if = WhatIfIndexCreation(database_connector)
        # convert MB to bytes
        self.disk_constraint = self.parameters['budget'] * 1000000
        self.seconds_limit = self.parameters['try_variation_seconds_limit']
        self.maximum_remove = self.parameters['try_variation_maximum_remove']

    def _calculate_best_indexes(self, workload):
        logging.info('Calculating best indexes IBM')
        query_results, candidates = self._exploit_virtual_indexes(workload)
        indexes_benefit_to_size = self._calculate_index_benefits(candidates,
                                                                 query_results)
        self._combine_subsumed(indexes_benefit_to_size)

        selected_indexes = []
        disk_usage = 0
        for index in indexes_benefit_to_size:
            if disk_usage + index['size'] <= self.disk_constraint:
                selected_indexes.append(index)
                disk_usage += index['size']
        self._try_variations(selected_indexes, indexes_benefit_to_size,
                             disk_usage, workload)
        return [x['index'] for x in selected_indexes]

    def _exploit_virtual_indexes(self, workload):
        query_results, index_candidates = {}, set()
        for query in workload.queries:
            plan = self.database_connector.get_plan(query)
            initial_cost = plan['Total Cost']
            recommended, cost = self._recommended_indexes(query)
            query_results[query] = {'initial_cost': initial_cost,
                                    'cost': cost,
                                    'indexes': recommended}
            index_candidates.update(recommended)
        return query_results, index_candidates

    def _recommended_indexes(self, query):
        indexes = self._possible_indexes(query)
        logging.debug('Simulating indexes')
        for index in indexes:
            self.what_if.simulate_index(index, store_size=True)
        plan = self.database_connector.get_plan(query)
        plan_string = str(plan)
        cost = plan['Total Cost']
        self.what_if.drop_all_simulated_indexes()

        recommended_indexes = []
        for index in indexes:
            if index.hypopg_name in plan_string:
                recommended_indexes.append(index)

        logging.debug(f'Recommended indexes found: {len(recommended_indexes)}')
        return recommended_indexes, cost

    def _possible_indexes(self, query):
        # "SAEFIS" or "BFI" see IBM paper
        # This implementation is "BFI"
        columns = query.columns
        logging.debug(f'\n{query}')
        logging.debug(f'indexable columns: {len(columns)}')
        max_columns = self.parameters['max_index_columns']
        possible_indexes = [[column] for column in columns]

        for previous_number_columns in range(1, max_columns):
            new_possible_indexes = []
            for possible_index in possible_indexes:
                if len(possible_index) < previous_number_columns:
                    continue
                for column in columns:
                    same_table = possible_index[0].table == column.table
                    if same_table and column not in possible_index:
                        new_possible_index = possible_index.copy()
                        new_possible_index.append(column)
                        new_possible_indexes.append(new_possible_index)
            possible_indexes.extend(new_possible_indexes)

        logging.debug(f'possible indexes: {len(possible_indexes)}')
        return [Index(p) for p in possible_indexes]

    def _calculate_index_benefits(self, candidates, query_results):
        indexes_benefit_to_size = []
        for index_candidate in candidates:
            benefit = 0
            for query, value in query_results.items():
                if index_candidate in value['indexes']:
                    # TODO adjust when having weights for queries
                    benefit += value['initial_cost'] - value['cost']
            size = index_candidate.estimated_size
            indexes_benefit_to_size.append({'index': index_candidate,
                                            'benefit_to_size': benefit / size,
                                            'size': size,
                                            'benefit': benefit})
        return sorted(indexes_benefit_to_size, reverse=True,
                      key=lambda x: x['benefit_to_size'])

    # "Combine any index subsumed
    # by an index with a higher ratio with that index."
    def _combine_subsumed(self, indexes):
        remove_at = set()
        for i in range(len(indexes)):
            index = indexes[i]['index']
            for j in range(i):
                higher_ratio_index = indexes[j]['index']
                # check if "better" index includes all columns of `index`
                # then remove `index`
                if all(c in higher_ratio_index.columns for c in index.columns):
                    remove_at.add(i)
        for remove_at_index in sorted(remove_at, reverse=True):
            del indexes[remove_at_index]

    def _try_variations(self, selected_indexes, indexes_benefit_to_size,
                        disk_usage, workload):
        logging.debug(f'Try variation for {self.seconds_limit} seconds')
        start_time = time.time()

        not_used_indexes = [x for x in indexes_benefit_to_size
                            if x not in selected_indexes]
        current_cost = self._evaluate_workload(selected_indexes, [], workload)
        logging.debug(f'Initial cost \t{current_cost}')

        while start_time + self.seconds_limit > time.time():
            disk_usage = sum([x['size'] for x in selected_indexes])
            # randomly choose indexes from current index set
            number_removed = random.randrange(1, self.maximum_remove)
            remove_at_indexes = list(range(len(selected_indexes)))
            random.shuffle(remove_at_indexes)
            remove_at_indexes = remove_at_indexes[:number_removed]

            # remove these chosen indexes
            removed = []
            for remove_at_index in sorted(remove_at_indexes, reverse=True):
                index = selected_indexes[remove_at_index]
                disk_usage -= index['size']
                del selected_indexes[remove_at_index]
                removed.append(index)

            # adding random unused indexes
            new_selected = []
            for i in range(number_removed):
                maximum_size = self.disk_constraint - disk_usage
                candidates = [x for x in not_used_indexes
                              if x['size'] <= maximum_size]
                if len(candidates) == 0:
                    break
                random.shuffle(candidates)
                selected_index = candidates[0]
                disk_usage += selected_index['size']
                new_selected.append(selected_index)
                not_used_indexes.remove(selected_index)

            # reevaluate new selected and replace if lower cost
            cost = self._evaluate_workload(selected_indexes,
                                           new_selected, workload)
            if cost < current_cost:
                not_used_indexes.extend(removed)
                selected_indexes.extend(new_selected)
                current_cost = cost
                logging.debug(f'Lower cost found \t{current_cost}')
            else:
                selected_indexes.extend(removed)
                not_used_indexes.extend(new_selected)

    def _evaluate_workload(self, selected, new_selected, workload):
        index_candidates = selected + new_selected
        index_candidates = [x['index'] for x in index_candidates]
        return self.cost_evaluation.calculate_cost(workload,
                                                   index_candidates)
