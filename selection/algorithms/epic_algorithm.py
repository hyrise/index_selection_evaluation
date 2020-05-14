from ..selection_algorithm import SelectionAlgorithm
from ..index import Index
import logging

# cost_estimation: 'whatif' or 'acutal_runtimes'
# Index combination budget in MB
DEFAULT_PARAMETERS = {
    'cost_estimation': 'whatif',
    'budget': 10,
    'min_cost_improvement': 1.01,
    'max_index_columns': 4
}


class EPICAlgorithm(SelectionAlgorithm):
    def __init__(self, database_connector, parameters=None):
        if parameters is None:
            parameters = {}
        SelectionAlgorithm.__init__(self, database_connector, parameters,
                                    DEFAULT_PARAMETERS)
        # MB to Bytes
        self.budget = self.parameters['budget'] * 1000000
        self.max_index_columns = self.parameters['max_index_columns']
        self.workload = None
        self.min_cost_improvement = self.parameters['min_cost_improvement']

    def _calculate_best_indexes(self, workload):
        logging.info('Calculating best indexes EPIC')
        self.workload = workload
        single_attribute_index_candidates = self.workload.potential_indexes()
        multi_attribute_index_candidates = single_attribute_index_candidates.copy(
        )

        # Current index combination
        index_combination = []
        index_combination_size = 0
        # Best index combination during evaluation step
        best = {'combination': [], 'benefit_to_size_ratio': 0, 'cost': None}

        current_cost = self.cost_evaluation.calculate_cost(self.workload,
                                                           index_combination,
                                                           store_size=True)
        self.initial_cost = current_cost
        # Breaking when no cost improvement
        while True:
            single_attribute_index_candidates = self._get_candidates_within_budget(
                index_combination_size, single_attribute_index_candidates)
            for candidate in single_attribute_index_candidates:
                # Only single column index generation
                if candidate not in index_combination:
                    self._evaluate_combination(index_combination + [candidate],
                                               best, current_cost)

            for candidate in multi_attribute_index_candidates:
                # Multi column indexes are generated by attaching columns to existing indexes
                self._attach_to_indexes(index_combination, candidate, best,
                                        current_cost)
            if best['benefit_to_size_ratio'] <= 0:
                break
            index_combination = best['combination']
            best['benefit_to_size_ratio'] = 0
            current_cost = best['cost']
            index_combination_size = sum(index.estimated_size
                                         for index in index_combination)

        return index_combination

    def _attach_to_indexes(self, index_combination, candidate, best,
                           current_cost):
        assert candidate.is_single_column(
        ) is True, 'Attach to indexes called with multi column index'

        for position, index in enumerate(index_combination):
            if len(index.columns) >= self.max_index_columns:
                continue
            if index.appendable_by(candidate):
                new_index = Index(index.columns + candidate.columns)
                if new_index in index_combination:
                    continue
                new_combination = index_combination.copy()
                # We do not replace, but del and append to keep track of their addition order
                del new_combination[position]
                new_combination.append(new_index)
                self._evaluate_combination(
                    new_combination, best, current_cost,
                    index_combination[position].estimated_size)

    def _get_candidates_within_budget(self, index_combination_size,
                                      candidates):
        new_candidates = []
        for candidate in candidates:
            if (candidate.estimated_size is
                    None) or (candidate.estimated_size + index_combination_size
                              <= self.budget):
                new_candidates.append(candidate)
        return new_candidates

    def _evaluate_combination(self,
                              index_combination,
                              best,
                              current_cost,
                              old_index_size=0):
        cost = self.cost_evaluation.calculate_cost(self.workload,
                                                   index_combination,
                                                   store_size=True)
        if (cost * self.min_cost_improvement) >= current_cost:
            return
        benefit = current_cost - cost
        new_index = index_combination[-1]
        new_index_size_difference = new_index.estimated_size - old_index_size
        if new_index_size_difference == 0:
            new_index_size_difference = 1

        ratio = benefit / new_index_size_difference

        total_size = sum(index.estimated_size for index in index_combination)

        if ratio > best['benefit_to_size_ratio'] and total_size <= self.budget:
            logging.debug(f'new best cost and size: {cost}\t'
                          f'{round(total_size / 1000000, 2)}MB')
            best['combination'] = index_combination
            best['benefit_to_size_ratio'] = ratio
            best['cost'] = cost
