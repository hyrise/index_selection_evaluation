import pyhdb
import re
import subprocess
import json
import logging


from ..database_connector import DatabaseConnector


class HanaDatabaseConnector(DatabaseConnector):
    def __init__(self, db_name, autocommit=False, columns=[]):
        DatabaseConnector.__init__(self, db_name, autocommit=autocommit,
                                   columns=columns)
        self.db_system = 'hana'
        self._connection = None

        # `db_name` is the schema name
        if not self.db_name:
            self.db_name = 'SYSTEM'

        logging.getLogger(name='pyhdb').setLevel(logging.ERROR)
        self.read_connection_file()

        self.create_connection()
        self._alter_configuration()

        logging.debug('HANA connector created: {}'.format(db_name))

    def read_connection_file(self):
        with open('database_connection.json', 'r') as file:
            connection_data = json.load(file)
        self.host = connection_data['host']
        self.port = connection_data['port']
        self.db_user = connection_data['db_user']
        self.db_user_password = connection_data['db_user_password']
        self.import_directory = connection_data['import_directory']
        self.ssh_user = connection_data['ssh_user']

    def _alter_configuration(self):
        logging.info('Setting HANA variables')
        variables = [('indexserver.ini', 'SYSTEM', 'datastatistics',
                      'dev_force_use_non_runtime_datastatistics', 'true'),
                     ('global.ini', 'SYSTEM', 'datastatistics',
                      'dev_force_use_non_runtime_datastatistics', 'true'),
                     ('indexserver.ini', 'database', 'import_export',
                      'enable_csv_import_path_filter', 'false')]
        string = ("alter system alter configuration ('{}', '{}') "
                  "set ('{}','{}')='{}' WITH RECONFIGURE")

        for database_variable in variables:
            execute_string = string.format(*database_variable)
            logging.debug(execute_string)
            self.exec_only(execute_string)

    def create_connection(self):
        if self._connection:
            self.close()
        self._connection = pyhdb.connect(
            host=self.host,
            port=self.port,
            user=self.db_user,
            password=self.db_user_password
        )
        self._connection.autocommit = self.autocommit
        self._cursor = self._connection.cursor()
        self.exec_only('set schema {}'.format(self.db_name))

    def database_names(self):
        result = self.exec_fetch('select schema_name from schemas', False)
        return [x[0].lower() for x in result]

    def enable_simulation(self):
        create_schema = f'create schema {self.db_name}_empty'
        self.exec_only(create_schema)
        self.exec_only(f'set schema {self.db_name}_empty')
        self.create_tables()

    def update_query_text(self, text):
        # TODO 'tpch' / 'tpcds' custom rules
        text = text.replace(';\nlimit ', ' limit ').replace('limit -1', '')
        text = self._replace_interval_by_function(text, 'day')
        text = self._replace_interval_by_function(text, 'month')
        text = self._replace_interval_by_function(text, 'year')
        text = self._change_substring_syntax(text)
        return text

    def _replace_interval_by_function(self, text, token):
        text = re.sub(rf"date '(.+)' (.) interval '(.*)' {token}",
                      rf"add_{token}s(to_date('\1','YYYY-MM-DD'),\2\3)", text)
        return text

    def _change_substring_syntax(self, text):
        text = re.sub(r"substring\((.+) from (.+) for (.+)\)",
                      r"substring(\1, \2, \3)", text)
        return text

    def create_database(self, database_name):
        self.exec_only('Create schema {}'.format(database_name))
        logging.info('Database (schema) {} created'.format(database_name))

    def import_data(self, table, path):
        scp_target = f'{self.ssh_user}@{self.host}:{self.import_directory}'
        # TODO pass scp output to logger
        subprocess.run(['scp', path, scp_target])
        csv_file = self.import_directory + '/' + path.split('/')[-1]
        import_statement = (f"import from csv file '{csv_file}' "
                            f"into {table} with record delimited by '\\n' "
                            "field delimited by '|'")
        logging.debug('Import csv statement {}'.format(table))
        self.exec_only(import_statement)

    def indexable_columns(self, query):
        indexable_columns = []
        plan = self.get_plan(query)
        plan_items = ''.join([x[1].read() for x in plan])
        for column in self.columns:
            if column.name in plan_items.lower():
                indexable_columns.append(column)
        return indexable_columns

    def get_plan(self, query):
        query_text = self._prepare_query(query)
        statement_name = f'{self.db_name}_q{query.nr}'
        statement = (f"explain plan set "
                     f"statement_name='{statement_name}' for "
                     f"{query_text}")
        try:
            self.exec_only(statement)
        except Exception as e:
            # pdb returns this even if the explain statement worked
            if str(e) != 'Invalid or unsupported function code received: 7':
                raise e
        # TODO store result in dictionary-like format
        result = self.exec_fetch('select operator_name, operator_details, '
                                 'output_size, subtree_cost, execution_engine '
                                 'from explain_plan_table '
                                 f"where statement_name='{statement_name}'",
                                 one=False)
        self.exec_only('delete from explain_plan_table where '
                       f"statement_name='{statement_name}'")
        self._cleanup_query(query)
        return result

    def _cleanup_query(self, query):
        for query_statement in query.text.split(';'):
            if 'drop view' in query_statement:
                self.exec_only(query_statement)

    def get_cost(self, query):
        # TODO how to get cost when simulating indexes
        query_plan = self.get_plan(query)
        print(query_plan)
        total_cost = query_plan[0][3]
        return total_cost

    def drop_indexes(self):
        logging.info('Dropping indexes')
        statement = 'select index_name from indexes where schema_name='
        statement += f"'{self.db_name.upper()}'"
        indexes = self.exec_fetch(statement, one=False)
        for index in indexes:
            index_name = index[0]
            drop_stmt = 'drop index {}'.format(index_name)
            logging.debug('Dropping index {}'.format(index_name))
            self.exec_only(drop_stmt)

    def create_statistics(self):
        logging.info('HANA')

    def create_index(self, index):
        table_name = index.columns[0].table
        statement = (f'create index {index.index_idx()} '
                     f'on {table_name} ({index.joined_column_names()})')
        self.exec_only(statement)
        #  size = self.exec_fetch(f'select relpages from pg_class c '
        #                         f'where c.relname = \'{index.index_idx()}\'')
        #  size = size[0]
        #  index.estimated_size = size * 8 * 1024


    #  def copy_data(self, table, text, delimiter='|'):
    #      if self.db_system != 'postgres':
    #          raise NotImplementedError('only postgres')
    #      self._cursor.copy_from(text, table, sep=delimiter, null='')

    #  def indexes_size(self):
    #      if self.db_system != 'postgres':
    #          raise NotImplementedError('only postgres')

    #      # Returns size in bytes
    #      #  statement = ("select sum(pg_indexes_size(table_name)) from "
    #      #               "(select table_name from information_schema.tables "
    #      #               "where table_schema='public') as all_tables")
    #      #  result = self.exec_fetch(statement)
    #      #  return result[0]
    #      # TODO pg_indexes_size needs oid,
    #      # example: select pg_indexes_size(16415);

    #      return 0

    #  def drop_database(self, database_name):
    #      if self.db_system != 'postgres':
    #          raise NotImplementedError('only postgres')
    #      self.exec_only('drop database {}'.format(database_name))
    #      logging.info('Database {} dropped'.format(database_name))

    #  def create_statistics(self):
    #      if self.db_system != 'postgres':
    #          raise NotImplementedError('only postgres')
    #      logging.info('Postgres: Run `vacuum analyze`')
    #      self.commit()
    #      self._connection.autocommit = True
    #      self.exec_only('vacuum analyze')
    #      self._connection.autocommit = self.autocommit

    #  def supports_index_simulation(self):
    #      if self.db_system == 'postgres':
    #          return True
    #      return False


    #  def simulate_index(self, index):
    #      if self.db_system != 'postgres':
    #          raise NotImplementedError('only postgres')
    #      table_name = index.columns[0].table
    #      statement = ("select * from hypopg_create_index( "
    #                   f"'create index on {table_name} "
    #                   f"({index.joined_column_names()})')")
    #      result = self.exec_fetch(statement)
    #      return result


    #  def drop_index(self, index):
    #      if self.db_system != 'postgres':
    #          raise NotImplementedError('only postgres')
    #      statement = f'drop index {index.index_idx()}'
    #      self.exec_only(statement)

    #  def drop_indexes(self):
    #      if self.db_system != 'postgres':
    #          raise NotImplementedError('only postgres')
    #      logging.info('Dropping indexes')
    #      stmt = "select indexname from pg_indexes where schemaname='public'"
    #      indexes = self.exec_fetch(stmt, one=False)
    #      for index in indexes:
    #          index_name = index[0]
    #          if not index_name.endswith('_pkey'):
    #              drop_stmt = 'drop index {}'.format(index_name)
    #              logging.debug('Dropping index {}'.format(index_name))
    #              self.exec_only(drop_stmt)

    #  def exec_query(self, query, timeout=None, cost_evaluation=False):
    #      if self.db_system != 'postgres':
    #          raise NotImplementedError('only postgres supports timeout yet')
    #      # Committing to not lose indexes after timeout
    #      if not cost_evaluation:
    #          self._connection.commit()
    #      query_text = self._prepare_query(query)
    #      if timeout:
    #          set_timeout = "set statement_timeout={}".format(timeout * 1000)
    #          self.exec_only(set_timeout)
    #      statement = f'explain (analyze, buffers, format json) {query_text}'
    #      try:
    #          plan = self.exec_fetch(statement, one=True)[0][0]['Plan']
    #          result = plan['Actual Total Time'], plan
    #      except Exception as e:
    #          logging.error(f'{query.nr}, {e}')
    #          self._connection.rollback()
    #          result = None, self.get_plan(query)
    #      # Disable timeout
    #      self._cursor.execute('set statement_timeout = 0')
    #      self._cleanup_query(query)
    #      return result

    #  def _prepare_query(self, query):
    #      for query_statement in query.text.split(';'):
    #          if 'create view' in query_statement:
    #              self.exec_only(query_statement)
    #          elif 'select' in query_statement:
    #              return query_statement

    #  def _cleanup_query(self, query):
    #      for query_statement in query.text.split(';'):
    #          if 'drop view' in query_statement:
    #              # self.exec_only(query_statement)
    #              # Rollback to not have too many transaction locks
    #              # drop view and commit would be an alternative
    #              self.rollback()

    #  def close(self):
    #      self._connection.close()
    #      logging.debug('Database connector closed: {}'.format(self.db_name))
    #      del self

    #  def rollback(self):
    #      if self.db_system != 'postgres':
    #          raise NotImplementedError('only postgres')
    #      self._connection.rollback()

    #  def get_cost(self, query):
    #      if self.db_system != 'postgres':
    #          raise NotImplementedError('only postgres supports cost estimation')
    #      query_plan = self.get_plan(query)
    #      total_cost = query_plan['Total Cost']
    #      return total_cost

    #  def indexable_columns(self, query):
    #      indexable_columns = []
    #      plan = self.get_plan(query)
    #      for column in self.columns:
    #          if column.name in str(plan):
    #              indexable_columns.append(column)
    #      return indexable_columns

    #  def get_plan(self, query):
    #      query_text = self._prepare_query(query)
    #      statement = 'explain (format json) {}'.format(query_text)
    #      query_plan = self.exec_fetch(statement)[0][0]['Plan']
    #      self._cleanup_query(query)
    #      return query_plan

    #  def number_of_indexes(self):
    #      if self.db_system != 'postgres':
    #          raise NotImplementedError('only postgres')
    #      statement = """select count(*) from pg_indexes
    #                     where schemaname = 'public'"""
    #      result = self.exec_fetch(statement)
    #      return result[0]
