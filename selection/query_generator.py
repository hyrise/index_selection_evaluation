import logging
import platform
import os
import subprocess
import re

from .workload import Query


class QueryGenerator:
    def __init__(self, benchmark_name, scale_factor, db_connector):
        self.scale_factor = scale_factor
        self.benchmark_name = benchmark_name
        self.db_connector = db_connector
        self.queries = []

        self.generate()

    def filter_queries(self, query_ids):
        self.queries = [query for query in self.queries
                        if query.nr in query_ids]

    def _generate_tpch(self):
        logging.info('Generating TPC-H Queries')
        self._run_make()
        # Using default parameters (`-d`)
        queries_string = self._run_command(['./qgen', '-c', '-d', '-s',
                                            str(self.scale_factor)],
                                           return_output=True)
        for query in queries_string.split('Query (Q'):
            query_id_and_text = query.split(')\n', 1)
            if len(query_id_and_text) == 2:
                query_id, text = query_id_and_text
                text = self._add_alias_subquery(text)
                query_id = int(query_id)
                self.queries.append(Query(query_id, text,
                                          self.db_connector))
        logging.info('Queries generated')

    #  def _generate_tpcds(self):
    #      logging.info('Generating TPC-DS Queries')
    #      self._run_make()
    #      command = ['cp -n ../query_templates/*.tpl ../../tpcds-templates']
    #      self._run_command(command, shell=True)
    #      # How to use different parameters
        # dialects: ansi, db2, netezza, oracle, sqlserver
    #      command = ['./dsqgen', '-DIRECTORY', '../../tpcds-templates',
    #                 '-INPUT', '../query_templates/templates.lst',
    #                 '-DIALECT', 'netezza', '-QUALIFY', 'Y',
    #                 '-OUTPUT_DIR', '../..']
    #      self._run_command(command)
    #      with open('query_0.sql', 'r') as file:
    #          queries_string = file.read()
    #      for query_string in queries_string.split('-- start query'):
    #          id_and_text = query_string.split('.tpl\n', 1)
    #          if len(id_and_text) != 2:
    #              continue
    #          query_id = int(id_and_text[0].split('using template query')[-1])
    #          query_text = id_and_text[1]
    #          query_text = re.sub(r" ([0-9]+) days\)", r" interval '\1 days')",
    #                              query_text)
    #          query_text = self._add_alias_subquery(query_text)
    #          query = Query(query_id, query_text,
    #                        self.db_connector)
    #          self.queries.append(query)

    # PostgreSQL requires an alias for subqueries
    def _add_alias_subquery(self, query_text):
        text = query_text.lower()
        positions = []
        for match in re.finditer(r'((from)|,)[  \n]*\(', text):
            counter = 1
            pos = match.span()[1]
            while counter > 0:
                char = text[pos]
                if char == '(':
                    counter += 1
                elif char == ')':
                    counter -= 1
                pos += 1
            next_word = query_text[pos:].lstrip().split(' ')[0].split('\n')[0]
            if next_word[0] in [')', ','] or next_word in ['limit',
                                                           'order', 'where']:
                positions.append(pos)
        for pos in sorted(positions, reverse=True):
            query_text = query_text[:pos] + ' as alias123 ' + query_text[pos:]
        return query_text

    def _run_make(self):
        if 'qgen' not in self._files() and 'dsqgen' not in self._files():
            logging.info('Running make in {}'.format(self.directory))
            self._run_command(self.make_command)
        else:
            logging.debug('No need to run make')

    def _run_command(self, command, return_output=False, shell=False):
        env = os.environ.copy()
        env['DSS_QUERY'] = 'queries'
        p = subprocess.Popen(command, cwd=self.directory,
                             stdout=subprocess.PIPE,
                             stderr=subprocess.STDOUT, shell=shell,
                             env=env)
        with p.stdout:
            output_string = p.stdout.read().decode('utf-8')
        p.wait()
        if return_output:
            return output_string
        else:
            logging.debug('[SUBPROCESS OUTPUT] ' + output_string)

    def _files(self):
        return os.listdir(self.directory)

    def generate(self):
        if self.benchmark_name == 'tpch':
            self.directory = './tpch-kit/dbgen'
            # DBMS in tpch-kit dbgen Makefile:
            # INFORMIX, DB2, TDAT (Teradata),
            # SQLSERVER, SYBASE, ORACLE, VECTORWISE, POSTGRESQL
            self.make_command = ['make', 'DATABASE=POSTGRESQL']
            if platform.system() == 'Darwin':
                self.make_command.append('OS=MACOS')

            self._generate_tpch()
        elif self.benchmark_name == 'tpcds':
            self.directory = './tpcds-kit/tools'
            self.make_command = ['make']
            if platform.system() == 'Darwin':
                self.make_command.append('OS=MACOS')

            self._generate_tpcds()
        else:
            raise NotImplementedError('only tpch/tpcds implemented.')
