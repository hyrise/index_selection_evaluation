import logging

from ..index import Index
from ..selection_algorithm import SelectionAlgorithm

DEFAULT_PARAMETERS = {"example_parameter": 3}


class ExampleAlgorithm(SelectionAlgorithm):
    def __init__(self, database_connector, global_config, name, parameters):
        SelectionAlgorithm.__init__(
            self, database_connector, parameters, global_config, name, DEFAULT_PARAMETERS
        )
        self.example_parameter = self.parameters["example_parameter"]

    def _calculate_best_indexes(self, workload):
        logging.info("Start example index selection algorithm")
        columns = workload.indexable_columns()
        indexes = []
        for i in range(self.example_parameter):
            indexes.append(Index(columns[i]))
        return indexes
