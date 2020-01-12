from ..selection_algorithm import SelectionAlgorithm
from ..what_if_index_creation import WhatIfIndexCreation
from ..index import Index
import logging
import time
import random
import itertools


DEFAULT_PARAMETERS = {'max_index_columns': 3, 'budget': 500,
                      'try_variation_seconds_limit': 10}


class IBMAlgorithm(SelectionAlgorithm):
    def __init__(self, database_connector, parameters):
        SelectionAlgorithm.__init__(self, database_connector, parameters,
                                    DEFAULT_PARAMETERS)
        self.what_if = WhatIfIndexCreation(database_connector)

    def _calculate_best_indexes(self, workload):
        logging.info('Calculating best indexes IBM')

        index_results = {}
        query_results = {}

        for query in workload.queries:
            plan = self.database_connector.get_plan(query)
            initial_cost = plan['Total Cost']
            recommended, cost = self.recommended_indexes(query)
            query_results[query] = {'initial_cost': initial_cost,
                                    'cost': cost,
                                    'indexes': [str(x) for x in recommended]}

            # TODO use object as key and add equal method for Index class
            # implement __eq__ and __hash__ for Index class
            index_results.update({str(x): {'obj': x}
                                  for x in recommended})

        # q.indexes und results keys irgendwie als objekte und nicht strings?
        indexes_benefit_to_size = []

        for index_string, index_values in index_results.items():
            benefit = 0
            for query, query_values in query_results.items():
                if index_string in query_values['indexes']:
                    # TODO adjust when having weight for queries
                    benefit += query_values['initial_cost'] \
                               - query_values['cost']
            # index_values['benefit'] = benefit
            size = index_values['obj'].estimated_size

            indexes_benefit_to_size.append({'index': index_values['obj'],
                                            'benefit_to_size': benefit / size,
                                            'size': size,
                                            'benefit': benefit})

        indexes_benefit_to_size = sorted(indexes_benefit_to_size, reverse=True,
                                         key=lambda x: x['benefit_to_size'])
        # print(indexes_benefit_to_size)
        self._combine_subsumed(indexes_benefit_to_size)

        selected_indexes = []
        disk_usage = 0
        # convert to bytes
        disk_constraint = self.parameters['budget'] * 1000000
        for index in indexes_benefit_to_size:
            if disk_usage + index['size'] <= disk_constraint:
                selected_indexes.append(index)
                disk_usage += index['size']
                # print('fit', index['size'])
            # else:
            #     print('----')
            #     print(disk_usage)
            #     print(index['size'])
        self._try_variations(selected_indexes, indexes_benefit_to_size,
                             disk_usage, disk_constraint, workload)
        return [x['index'] for x in selected_indexes]

    def _try_variations(self, selected_indexes, indexes_benefit_to_size,
                        disk_usage, disk_constraint, workload):
        seconds = self.parameters['try_variation_seconds_limit']
        logging.debug(f'Try variation for {seconds} seconds')
        start_time = time.time()
        print(start_time)


        not_used_indexes = [x for x in indexes_benefit_to_size
                            if x not in selected_indexes]

        seed = 24
        maximum_remove = 4

        random.seed(seed)

        current_cost = self._evaluate_workload(selected_indexes, [], workload)
        print('current cost', current_cost)

        counter = 0

        # ----- loop
        while start_time + seconds > time.time():
            disk_usage = sum([x['size'] for x in selected_indexes])

            # print('size sel', len(selected_indexes))
            # print('size not', len(not_used_indexes))
            remove_indexes = random.randrange(1, maximum_remove)
            # print('remove x ', remove_indexes)

            remove_at_indexes = list(range(len(selected_indexes)))
            random.shuffle(remove_at_indexes)
            remove_at_indexes = remove_at_indexes[:remove_indexes]
            # print(remove_at_indexes)

            removed = []
            for remove_at_index in sorted(remove_at_indexes, reverse=True):
                index = selected_indexes[remove_at_index]
                size = index['size']
                disk_usage -= size
                del selected_indexes[remove_at_index]
                removed.append(index)
                # print(disk_usage)

            new_selected = []
            while True:
                maximum_size = disk_constraint - disk_usage
                candidates = [x for x in not_used_indexes if x['size'] <= maximum_size]
                if len(candidates) == 0:
                    break
                random.shuffle(candidates)
                selected_index = candidates[0]
                disk_usage += selected_index['size']
                new_selected.append(selected_index)
                not_used_indexes.remove(selected_index)
                # print('sel', selected_index)

            cost = self._evaluate_workload(selected_indexes, new_selected, workload)
            # print('new cost', cost)

            # if new selected besser als alt:
            if cost < current_cost:
                print('----')
                not_used_indexes.extend(removed)
                selected_indexes.extend(new_selected)
                print('better cost', cost)
                print(removed)
                print(new_selected)
                print(disk_usage)
                current_cost = cost
            else:
                selected_indexes.extend(removed)
                not_used_indexes.extend(new_selected)

            counter += 1



        # print('len sel: ', len(selected_indexes))
        # print('len all ind: ', len(indexes_benefit_to_size))
        # print(selected_indexes)
        # print('')
        # print('')
        # print(indexes_benefit_to_size)
        # print('len', len(not_used_indexes))

    def _evaluate_workload(self, selected, new_selected, workload):
        for index in selected:
            self.what_if.simulate_index(index['index'])
        for index in new_selected:
            self.what_if.simulate_index(index['index'])
        cost = 0
        for query in workload.queries:
            plan = self.database_connector.get_plan(query)
            plan_string = str(plan)
            cost += plan['Total Cost']
        self.what_if.drop_all_simulated_indexes()
        return cost

    def _combine_subsumed(self, indexes):
        # "Combine any index subsumed
        # by an index with a higher ratio with that index."
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

    def possible_indexes(self, query):
        # "SAEFIS" or "BFI" see IBM paper
        # This implementation is "BFI"
        # TODO another way is to stop "after a certain maximum
        # number of indexes is reached"
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

    def recommended_indexes(self, query):
        indexes = self.possible_indexes(query)
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
