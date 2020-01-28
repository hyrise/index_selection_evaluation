from ...index import Index
import logging


class MultiColumnIndexGeneration:
    def __init__(self, potential_indexes):
        logging.debug('Init MultiColumnIndexGeneration')
        self.potential_indexes = potential_indexes
        self.singlecolumn_indexes = [c for c in potential_indexes if
                                     c.singlecolumn()]

    def create(self, indexes):
        multicolumn_candidates = []
        for index in indexes:
            for candidate in self.singlecolumn_indexes:
                candidate_column = candidate.columns[0]
                if candidate_column in index.columns:
                    continue
                if candidate_column.table == index.table():
                    new_index = Index(index.columns + (candidate_column,))
                    multicolumn_candidates.append(new_index)
        return multicolumn_candidates
