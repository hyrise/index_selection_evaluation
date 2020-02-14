from ..selection_algorithm import SelectionAlgorithm
from ..index import Index
import logging

# cost_estimation: 'whatif' or 'acutal_runtimes'
# Index combination budget in MB
DEFAULT_PARAMETERS = {'cost_estimation': 'whatif', 'budget': 10}


class EPICAlgorithm(SelectionAlgorithm):
    def __init__(self, database_connector, parameters):
        SelectionAlgorithm.__init__(self, database_connector, parameters,
                                    DEFAULT_PARAMETERS)
        # MB to Bytes
        self.budget = self.parameters['budget'] * 1000000

    def _calculate_best_indexes(self, workload):
        logging.info('Calculating best indexes EPIC')
        single_attribute_index_candidates = workload.potential_indexes()

        # Current index combination
        index_combination = []
        index_combination_size = 0
        # Best index combination during evaluation step
        best = {'combination': [], 'benefit_to_size_ratio': 0}

        # Breaking when no cost improvement
        while True:
            initial_cost = self._retrieve_cost(workload, index_combination)

            for candidate in single_attribute_index_candidates:
                # Candidate not used anymore if too large for budget
                if (candidate.estimated_size
                        and index_combination_size + candidate.estimated_size >
                        self.budget):
                    single_attribute_index_candidates.remove(candidate)
                    continue

                if candidate not in index_combination:
                    self._evaluate_combination(index_combination + [candidate],
                                               best, workload, initial_cost)

                for i, index in enumerate(index_combination):
                    if index.appendable_by(candidate):
                        new_index = Index(index.columns + candidate.columns)
                        if new_index in index_combination:
                            continue
                        new_combination = index_combination.copy()
                        new_combination[i] = new_index
                        self._evaluate_combination(new_combination, best,
                                                   workload, initial_cost)
            if best['benefit_to_size_ratio'] == 0:
                break
            index_combination = best['combination']
            best['benefit_to_size_ratio'] = 0
            index_combination_size = sum(x.estimated_size
                                         for x in index_combination)
        return index_combination

    def _evaluate_combination(self, index_combination, best, workload,
                              initial_cost):
        cost = self._retrieve_cost(workload, index_combination)
        benefit = initial_cost - cost
        size = sum(x.estimated_size for x in index_combination)
        ratio = benefit / size

        if ratio > best['benefit_to_size_ratio'] and size <= self.budget:
            logging.debug(f'new best cost and size: {cost}\t'
                          f'{round(size/1000000, 2)}MB')
            best['combination'] = index_combination
            best['benefit_to_size_ratio'] = ratio

    def _retrieve_cost(self, workload, indexes):
        cost = self.cost_evaluation.calculate_cost(workload,
                                                   indexes,
                                                   store_size=True)
        return cost

    # The cost to get to the new index combination, i.e. cost of
    # adding new indexes and dropping existing unneeded indexes
    def _retrieve_reconfig_cost(self, new, existing_indexes=[]):
        return 0
