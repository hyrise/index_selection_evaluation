import logging


class DatabaseConnector:
    def __init__(self, db_name, autocommit=False):
        self.db_name = db_name
        self.autocommit = autocommit
        logging.debug('Database connector created: {}'.format(db_name))

    def exec_only(self, statement):
        self._cursor.execute(statement)

    def exec_fetch(self, statement, one=True):
        self._cursor.execute(statement)
        if one:
            return self._cursor.fetchone()
        return self._cursor.fetchall()

    def enable_simulation(self):
        pass

    def commit(self):
        self._connection.commit()

    def close(self):
        self._connection.close()
        logging.debug('Database connector closed: {}'.format(self.db_name))

    def rollback(self):
        self._connection.rollback()

    def drop_index(self, index):
        statement = f'drop index {index.index_idx()}'
        self.exec_only(statement)

    def _prepare_query(self, query):
        for query_statement in query.text.split(';'):
            if 'create view' in query_statement:
                try:
                    self.exec_only(query_statement)
                except Exception as e:
                    logging.error(e)
            elif 'select' in query_statement or 'SELECT' in query_statement:
                return query_statement

    def table_exists(self, table_name):
        raise NotImplementedError

    def database_exists(self, database_name):
        raise NotImplementedError

    def drop_database(self, database_name):
        raise NotImplementedError

    def create_statistics(self):
        raise NotImplementedError

    def set_random_seed(self, value):
        raise NotImplementedError