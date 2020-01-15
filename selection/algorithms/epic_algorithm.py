from ..selection_algorithm import SelectionAlgorithm
from ..index import Index
import logging


DEFAULT_PARAMETERS = {'cost_estimation': 'whatif', 'budget': 10,
                      'number_steps': 20, 'pruning': True}


class EPICAlgorithm(SelectionAlgorithm):
    def __init__(self, database_connector, parameters):
        SelectionAlgorithm.__init__(self, database_connector, parameters,
                                    DEFAULT_PARAMETERS)
        # MB to Bytes
        self.budget = self.parameters['budget'] * 1000000

    def _calculate_best_indexes(self, workload):
        logging.info('Calculating best indexes EPIC')

        index_combination = []
        initial_cost = self._retrieve_cost(workload, index_combination)
        # # ?
        # initial_reconfig_cost = self._retrieve_reconfig_cost(indexes)
        best = [[], 0, initial_cost]

        singleattr_index_candidates = self.potential_indexes(workload)
        step = 0
        combi_size = None

        # while step < self.parameters['number_steps']:
        # Budget is used here. or breaking when no cost improvement
        while True:
            print(f'step: {step}')
            print(index_combination)
            # import pdb;pdb.set_trace()

            # make following optional
            # attributes = [item for sublist in index_combination
            #               for item in sublist.columns]
            # singleattr_index_candidates = [x for x
            #                                in singleattr_index_candidates
            #                                if x.columns[0] not in attributes]
            # --------
            print('combi size', combi_size)

            for candidate in singleattr_index_candidates:
                if combi_size:
                    est_size = candidate.estimated_size
                    if combi_size + est_size > self.budget:
                        # print('continue')
                        continue

                if candidate not in index_combination:
                    self._evaluate_combination(index_combination + [candidate],
                                               best, workload)

                for i, index in enumerate(index_combination):
                    if index.appendable_by(candidate):
                        columns = index.columns.copy()
                        columns.append(candidate.columns[0])
                        new_index = Index(columns)
                        if new_index.columns in [x.columns for x
                                                 in index_combination]:
                            continue
                        new_combination = index_combination.copy()
                        new_combination[i] = new_index
                        self._evaluate_combination(new_combination, best,
                                                   workload)
            if best[1] == 0:
                break
            index_combination = best[0]
            # Reset ratio for new step
            best[1] = 0
            step += 1
            combi_size = sum(x.estimated_size for x in index_combination)
        return index_combination

    def _evaluate_combination(self, index_combination, best, workload):
        cost = self._retrieve_cost(workload, index_combination)
        benefit = best[2] - cost
        size = sum(x.estimated_size for x in index_combination)
        ratio = benefit / size

        if ratio > best[1] and size <= self.budget:
            logging.debug(f'new best cost: {cost}\t{size}')
            best[0] = index_combination
            best[1] = ratio
            best[2] = cost

    def _retrieve_cost(self, workload, indexes):
        cost = self.cost_evaluation.calculate_cost(workload, indexes,
                                                   store_size=True)
        return cost

    def _retrieve_reconfig_cost(self, new, existing_indexes=[]):
        # TODO how?
        # size * ?
        return 0
