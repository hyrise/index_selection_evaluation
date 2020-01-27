import logging
import re
import platform
import os
import subprocess

from .workload import Query

class BaseQueryGenerator:
    def __init__(self, benchmark_name, scale_factor, db_connector,
                 query_ids):
        self.scale_factor = scale_factor
        self.benchmark_name = benchmark_name
        self.db_connector = db_connector
        self.queries = []
        self.query_ids = query_ids

        self.generate()

    def filter_queries(self, query_ids):
        self.queries = [query for query in self.queries
                        if query.nr in query_ids]

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
        raise NotImplementedError


class TPCDSQueryCreator(BaseQueryGenerator):
    def __init__(self, benchmark_name, scale_factor, db_connector,
                 query_ids):
        BaseQueryGenerator.__init__(self, benchmark_name, scale_factor,
            db_connector, query_ids)


    def _update_tpcds_query_text(self, query_text):
        query_text = query_text.replace(') returns', ') as returns')
        replaced_string = 'case when lochierarchy = 0'
        if replaced_string in query_text:
            new_string = re.search(r'grouping\(.*\)\+'
                                   r'grouping\(.*\) '
                                   r'as lochierarchy',
                                   query_text).group(0)
            new_string = new_string.replace(' as lochierarchy', '')
            new_string = 'case when ' + new_string + ' = 0'
            query_text = query_text.replace(replaced_string,
                                            new_string)
        return query_text


    def generate(self):
        self.directory = './tpcds-kit/tools'
        self.make_command = ['make']
        if platform.system() == 'Darwin':
            self.make_command.append('OS=MACOS')

        logging.info('Generating TPC-DS Queries')
        self._run_make()
        # dialects: ansi, db2, netezza, oracle, sqlserver
        command = ['./dsqgen', '-DIRECTORY', '../query_templates',
                   '-INPUT', '../query_templates/templates.lst',
                   '-DIALECT', 'netezza', '-QUALIFY', 'Y',
                   '-OUTPUT_DIR', '../..']
        self._run_command(command)
        with open('query_0.sql', 'r') as file:
            queries_string = file.read()
        for query_string in queries_string.split('-- start query'):
            id_and_text = query_string.split('.tpl\n', 1)
            if len(id_and_text) != 2:
                continue
            query_id = int(id_and_text[0].split('using template query')[-1])
            if self.query_ids and query_id not in self.query_ids:
                continue
            query_text = id_and_text[1]
            query_text = self._update_tpcds_query_text(query_text)
            query = Query(query_id, query_text,
                          self.db_connector)
            self.queries.append(query)


class TPCHQueryCreator(BaseQueryGenerator):
    def __init__(self, benchmark_name, scale_factor, db_connector,
                 query_ids):
        BaseQueryGenerator.__init__(self, benchmark_name, scale_factor,
            db_connector, query_ids)


    def generate(self):
        self.directory = './tpch-kit/dbgen'
        # DBMS in tpch-kit dbgen Makefile:
        # INFORMIX, DB2, TDAT (Teradata),
        # SQLSERVER, SYBASE, ORACLE, VECTORWISE, POSTGRESQL
        self.make_command = ['make', 'DATABASE=POSTGRESQL']
        if platform.system() == 'Darwin':
            self.make_command.append('OS=MACOS')

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
                query_id = int(query_id)
                if self.query_ids and query_id not in self.query_ids:
                    continue
                text = text.replace('\t', '')
                self.queries.append(Query(query_id, text,
                                          self.db_connector))
        logging.info('Queries generated')


class QueryLoader(BaseQueryGenerator):
    def __init__(self, benchmark_name, scale_factor, db_connector,
                 query_ids, query_directory):
        self.directory = query_directory

        BaseQueryGenerator.__init__(self, benchmark_name, scale_factor,
            db_connector, query_ids)

    def generate(self):
        print(self._files())
        for filename in self._files():
            if '.sql' not in filename or 'fkindexes' in filename or 'schema' in filename:
                continue
            query_id = filename.replace('.sql', '')

            with open(f"{self.directory}/{filename}", 'r') as file:
                query_text = file.read()
                query_text = query_text.replace('\t', '')
                self.queries.append(Query(query_id, query_text,
                                          self.db_connector))




