from .cost_evaluation import CostEvaluation
from .index import Index
import logging
import itertools


class SelectionAlgorithm:
    def __init__(self, database_connector, parameters, default_parameters=None):
        if default_parameters == None:
            default_parameters = {}
        logging.debug('Init selection algorithm')
        self.did_run = False
        self.parameters = parameters
        # Store default values for missing parameters
        for key, value in default_parameters.items():
            if key not in self.parameters:
                self.parameters[key] = value

        self.database_connector = database_connector
        self.database_connector.drop_indexes()
        self.cost_evaluation = CostEvaluation(database_connector)
        if 'cost_estimation' in self.parameters:
            estimation = self.parameters['cost_estimation']
            self.cost_evaluation.cost_estimation = estimation

    def calculate_best_indexes(self, workload):
        assert self.did_run is False, 'Selection algorithm can only run once.'
        self.did_run = True
        indexes = self._calculate_best_indexes(workload)
        self._log_cache_hits()
        self.cost_evaluation.complete_cost_estimation()

        return indexes

    def _calculate_best_indexes(self, workload):
        raise NotImplementedError('_calculate_best_indexes(self, '
                                  'workload) missing')

    def _log_cache_hits(self):
        hits = self.cost_evaluation.cache_hits
        requests = self.cost_evaluation.cost_requests
        logging.debug(f'Total cost cache hits:\t{hits}')
        logging.debug(f'Total cost requests:\t\t{requests}')
        if requests == 0:
            return
        ratio = round(hits * 100 / requests, 2)
        logging.debug(f'Cost cache hit ratio:\t{ratio}%')


class NoIndexAlgorithm(SelectionAlgorithm):
    def __init__(self, database_connector, parameters=None):
        if parameters == None:
            parameters = {}
        SelectionAlgorithm.__init__(self, database_connector, parameters)

    def _calculate_best_indexes(self, workload):
        return []


class AllIndexesAlgorithm(SelectionAlgorithm):
    def __init__(self, database_connector, parameters=None):
        if parameters == None:
            parameters = {}
        SelectionAlgorithm.__init__(self, database_connector, parameters)

    # Returns single column index for each indexable column
    def _calculate_best_indexes(self, workload):
        print(f"Singles: {len(workload.potential_indexes())}")

        for length in range(1, 3):
            unique = set()
            used_columns_per_table = {}
            for query in workload.queries:
                for column in query.columns:
                    if column.table not in used_columns_per_table:
                        used_columns_per_table[column.table] = set()
                    used_columns_per_table[column.table].add(column)

            count = 0
            for key, used_columns in used_columns_per_table.items():
                unique |= set(itertools.permutations(used_columns, length))
                count += len(set(itertools.permutations(used_columns, length)))
            # This can be utilized to generate the permutations for Microsoft SQL Server
            # for i in unique:
            #     column_str = ""
            #     for c in i:
            #         column_str += f"{c.name}##"
            #     print(f"{i[0].table}++{column_str}")
            print(f"{length}-column indexes: {count}")
