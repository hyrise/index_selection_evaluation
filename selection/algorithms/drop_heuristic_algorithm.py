from ..selection_algorithm import SelectionAlgorithm
import logging

# Algorithm stops when maximum number of indexes is reached
DEFAULT_PARAMETERS = {"max_indexes": 15, "cost_estimation": "whatif"}


class DropHeuristicAlgorithm(SelectionAlgorithm):
    def __init__(self, database_connector, parameters=None):
        if parameters is None:
            parameters = {}
        SelectionAlgorithm.__init__(
            self, database_connector, parameters, DEFAULT_PARAMETERS
        )

    def _calculate_best_indexes(self, workload):
        assert (
            self.parameters["max_indexes"] > 0
        ), "Calling the DropHeuristic with max_indexes < 1 does not make sense."
        logging.info("Calculating best indexes (drop heuristic)")
        logging.info("Parameters: " + str(self.parameters))

        # remaining_indexes is initialized as set of all potential indexes
        remaining_indexes = set(workload.potential_indexes())

        while len(remaining_indexes) > self.parameters["max_indexes"]:
            # Drop index that, when dropped, leads to lowest cost
            lowest_cost = None
            index_to_drop = None
            for index in remaining_indexes:
                cost = self.cost_evaluation.calculate_cost(
                    workload, remaining_indexes - set([index])
                )
                if not lowest_cost or cost < lowest_cost:
                    lowest_cost, index_to_drop = cost, index
            remaining_indexes.remove(index_to_drop)
            logging.info(
                (
                    f"Dropping Index: {index_to_drop}. "
                    f"{len(remaining_indexes)} indexes remaining."
                )
            )

        return remaining_indexes
