import logging


# Uses hypopg for postgreSQL
class WhatIfIndexCreation():
    def __init__(self, db_connector):
        logging.debug('Init WhatIfIndexCreation')

        self.simulated_indexes = {}
        self.db_connector = db_connector

    def simulate_index(self, potential_index, store_size=False):
        result = self.db_connector.simulate_index(potential_index)
        index_oid = result[0]
        index_name = result[1]
        self.simulated_indexes[index_oid] = index_name
        potential_index.hypopg_name = index_name
        potential_index.hypopg_oid = index_oid

        if store_size:
            self.store_estimated_size(potential_index, index_oid)

    def drop_simulated_index(self, index):
        oid = index.hypopg_oid
        del self.simulated_indexes[oid]
        self._drop_index_by_id(oid)

    def _drop_index_by_id(self, oid):
        statement = f'select * from hypopg_drop_index({oid})'
        self.db_connector.exec_only(statement)

    def store_estimated_size(self, index, index_oid):
        statement = f'select hypopg_relation_size({index_oid})'
        result = self.db_connector.exec_fetch(statement)
        index.estimated_size = result[0]

    def all_simulated_indexes(self):
        statement = 'select * from hypopg_list_indexes()'
        indexes = self.db_connector.exec_fetch(statement, one=False)
        return indexes

    # This is never used, we keep it for debugging reasons.
    def index_names(self):
        indexes = self.all_simulated_indexes()

        # Apparently, x[1] is the index' name
        return [x[1] for x in indexes]

    def drop_all_simulated_indexes(self):
        for key in self.simulated_indexes:
            self._drop_index_by_id(key)
        self.simulated_indexes = {}
