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
        self.simulated_indexes[result[0]] = index_name
        potential_index.hypopg_name = index_name
        potential_index.hypopg_oid = index_oid

        if store_size:
            self.estimated_size(potential_index, index_oid)

    def drop_simulated_index(self, index):
        oid = index.hypopg_oid
        del self.simulated_indexes[oid]
        statement = f'select * from hypopg_drop_index({oid})'
        self.db_connector.exec_only(statement)

    def estimated_size(self, index, index_oid):
        statement = f'select hypopg_relation_size({index_oid})'
        result = self.db_connector.exec_fetch(statement)
        index.estimated_size = result[0]

    def all_simulated_indexes(self):
        statement = 'select * from hypopg_list_indexes()'
        indexes = self.db_connector.exec_fetch(statement, one=False)
        return indexes

    def index_names(self):
        indexes = self.all_simulated_indexes()
        return [x[1] for x in indexes]

    def index_to_index_name(self, index):
        table_name = index.columns[0].table.name
        column_names = [c.name for c in index.columns]
        joined_columns = '_'.join(column_names)
        return table_name + '_' + joined_columns

    def drop_all_simulated_indexes(self):
        # `hypopg_reset()` does the same
        for key in self.simulated_indexes:
            statement = 'select * from hypopg_drop_index({})'.format(key)
            self.db_connector.exec_only(statement)
        self.simulated_indexes = {}
        # logging.debug('All simulated indexes dropped.')
