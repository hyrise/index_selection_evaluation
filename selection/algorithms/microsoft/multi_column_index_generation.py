from ...index import Index
import logging


class MultiColumnIndexGeneration:
    def __init__(self, potential_indexes):
        logging.debug('Init MultiColumnIndexGeneration')
        self.potential_indexes = potential_indexes
        self.single_column_indexes = [c for c in potential_indexes if
                                     c.is_single_column()]

    def create(self, indexes):
        multicolumn_candidates = []
        for index in indexes:
            for candidate in self.single_column_indexes:
                candidate_column = candidate.columns[0]
                if candidate_column in index.columns:
                    continue
                if candidate_column.table == index.table():
                    new_index = Index(index.columns + (candidate_column,))
                    multicolumn_candidates.append(new_index)
        return multicolumn_candidates
