from .configuration_enumeration import ConfigurationEnumeration
from ...workload import Workload
import logging


class CandidateIndexSelection():
    def __init__(self, cost_evaluation, microsoft_algorithm):
        logging.debug('Init CandidateIndexSelection')
        self.cost_evaluation = cost_evaluation
        self.microsoft_algorithm = microsoft_algorithm

    def select(self, workload, potential_indexes):
        candidates = []
        for query in workload.queries:
            logging.debug('Find candidates for query\t{}...'.format(query))
            # Create a workload consisting of one query
            query_workload = Workload([query], workload.database_name)
            indexes = self._filter_potential_indexes(query, potential_indexes)
            enumeration = ConfigurationEnumeration(indexes,
                                                   query_workload,
                                                   self.cost_evaluation,
                                                   self.microsoft_algorithm,
                                                   candidate_selection=True)
            candidates.extend(enumeration.enumerate())
        # Remove duplicates from candidates list
        candidates = list(set(candidates))
        logging.info('Number of candidates: {}'.format(len(candidates)))
        logging.info('Candidates: {}'.format(candidates))
        return candidates

    def _filter_potential_indexes(self, query, potential_indexes):
        indexes = []
        for index in potential_indexes:
            if all(c in query.columns for c in index.columns):
                indexes.append(index)
        return indexes
