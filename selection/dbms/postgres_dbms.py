import psycopg2
import logging


from ..database_connector import DatabaseConnector


class PostgresDatabaseConnector(DatabaseConnector):
    def __init__(self, db_name, autocommit=False, columns=[]):
        DatabaseConnector.__init__(self, db_name, autocommit=autocommit,
                                   columns=columns)
        self.db_system = 'postgres'
        self._connection = None

        if not self.db_name:
            self.db_name = 'postgres'
        self.create_connection()

        logging.debug('Postgres connector created: {}'.format(db_name))

    def create_connection(self):
        if self._connection:
            self.close()
        self._connection = psycopg2.connect('dbname={}'.format(self.db_name))
        self._connection.autocommit = self.autocommit
        self._cursor = self._connection.cursor()

    def enable_simulation(self):
        self.exec_only('create extension hypopg')
        self.commit()

    def database_names(self):
        result = self.exec_fetch('select datname from pg_database', False)
        return [x[0] for x in result]

    def update_query_text(self, text):
        text = text.replace(';\nlimit ', ' limit ').replace('limit -1', '')
        return text

    def create_database(self, database_name):
        if self.db_system != 'postgres':
            raise NotImplementedError('only postgres')
        self.exec_only('create database {}'.format(database_name))
        logging.info('Database {} created'.format(database_name))

    def import_data(self, table, path, delimiter='|'):
        with open(path, 'r') as file:
            self._cursor.copy_from(file, table, sep=delimiter, null='')

    def indexes_size(self):
        if self.db_system != 'postgres':
            raise NotImplementedError('only postgres')

        # Returns size in bytes
        #  statement = ("select sum(pg_indexes_size(table_name)) from "
        #               "(select table_name from information_schema.tables "
        #               "where table_schema='public') as all_tables")
        #  result = self.exec_fetch(statement)
        #  return result[0]
        # TODO pg_indexes_size needs oid,
        # example: select pg_indexes_size(16415);

        return 0

    def drop_database(self, database_name):
        if self.db_system != 'postgres':
            raise NotImplementedError('only postgres')
        self.exec_only('drop database {}'.format(database_name))
        logging.info('Database {} dropped'.format(database_name))

    def create_statistics(self):
        if self.db_system != 'postgres':
            raise NotImplementedError('only postgres')
        logging.info('Postgres: Run `vacuum analyze`')
        self.commit()
        self._connection.autocommit = True
        self.exec_only('vacuum analyze')
        self._connection.autocommit = self.autocommit

    def supports_index_simulation(self):
        if self.db_system == 'postgres':
            return True
        return False

    def simulate_index(self, index):
        if self.db_system != 'postgres':
            raise NotImplementedError('only postgres')
        table_name = index.columns[0].table
        statement = ("select * from hypopg_create_index( "
                     f"'create index on {table_name} "
                     f"({index.joined_column_names()})')")
        result = self.exec_fetch(statement)
        return result

    def create_index(self, index):
        if self.db_system != 'postgres':
            raise NotImplementedError('only postgres')
        table_name = index.columns[0].table
        statement = (f'create index {index.index_idx()} '
                     f'on {table_name} ({index.joined_column_names()})')
        self.exec_only(statement)
        size = self.exec_fetch(f'select relpages from pg_class c '
                               f'where c.relname = \'{index.index_idx()}\'')
        size = size[0]
        index.estimated_size = size * 8 * 1024

    def drop_indexes(self):
        if self.db_system != 'postgres':
            raise NotImplementedError('only postgres')
        logging.info('Dropping indexes')
        stmt = "select indexname from pg_indexes where schemaname='public'"
        indexes = self.exec_fetch(stmt, one=False)
        for index in indexes:
            index_name = index[0]
            # TODO drop primary key indexes
            if not index_name.endswith('_pkey'):
                drop_stmt = 'drop index {}'.format(index_name)
                logging.debug('Dropping index {}'.format(index_name))
                self.exec_only(drop_stmt)

    def exec_query(self, query, timeout=None, cost_evaluation=False):
        if self.db_system != 'postgres':
            raise NotImplementedError('only postgres supports timeout yet')
        # Committing to not lose indexes after timeout
        if not cost_evaluation:
            self._connection.commit()
        query_text = self._prepare_query(query)
        if timeout:
            set_timeout = "set statement_timeout={}".format(timeout * 1000)
            self.exec_only(set_timeout)
        statement = f'explain (analyze, buffers, format json) {query_text}'
        try:
            plan = self.exec_fetch(statement, one=True)[0][0]['Plan']
            result = plan['Actual Total Time'], plan
        except Exception as e:
            logging.error(f'{query.nr}, {e}')
            self._connection.rollback()
            result = None, self.get_plan(query)
        # Disable timeout
        self._cursor.execute('set statement_timeout = 0')
        self._cleanup_query(query)
        return result

    def _cleanup_query(self, query):
        for query_statement in query.text.split(';'):
            if 'drop view' in query_statement:
                self.exec_only(query_statement)
                self.commit()

    def get_cost(self, query):
        if self.db_system != 'postgres':
            raise NotImplementedError('only postgres supports cost estimation')
        query_plan = self.get_plan(query)
        total_cost = query_plan['Total Cost']
        return total_cost

    def indexable_columns(self, query):
        indexable_columns = []
        plan = self.get_plan(query)
        for column in self.columns:
            if column.name in str(plan):
                indexable_columns.append(column)
        return indexable_columns

    def get_plan(self, query):
        query_text = self._prepare_query(query)
        statement = 'explain (format json) {}'.format(query_text)
        query_plan = self.exec_fetch(statement)[0][0]['Plan']
        self._cleanup_query(query)
        return query_plan

    def number_of_indexes(self):
        if self.db_system != 'postgres':
            raise NotImplementedError('only postgres')
        statement = """select count(*) from pg_indexes
                       where schemaname = 'public'"""
        result = self.exec_fetch(statement)
        return result[0]
