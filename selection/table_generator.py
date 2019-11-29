from .workload import Table, Column

import logging
import platform
import subprocess
import os
import re


class TableGenerator:
    def __init__(self, benchmark_name, scale_factor, database_connector):
        self.scale_factor = scale_factor
        self.benchmark_name = benchmark_name
        self.db_connector = database_connector

        self.database_names = self.db_connector.database_names()
        self.tables = []
        self.columns = []
        self._prepare()
        if self.database_name() not in self.database_names:
            self._generate()
            self.create_database()
        else:
            logging.debug('Database with given scale factor already '
                          'existing')
        self._read_column_names()

    def database_name(self):
        name = 'indexselection_' + self.benchmark_name + '___'
        name += str(self.scale_factor).replace('.', '_')
        return name

    def _read_column_names(self):
        # TODO is this needed?
        # Read table and column names from 'create table' statements
        id = 0
        filename = self.directory + '/' + self.create_table_statements_file
        with open(filename, 'r') as file:
            data = file.read().lower()
        create_tables = data.split('create table ')[1:]
        for create_table in create_tables:
            splitted = create_table.split('(', 1)
            table = Table(splitted[0].strip())
            self.tables.append(table)
            # TODO regex split? ,[whitespace]\n
            for column in splitted[1].split(',\n'):
                name = column.lstrip().split(' ', 1)[0]
                if name == 'primary':
                    continue
                column_object = Column(id, name, table)
                table.columns.append(column_object)
                self.columns.append(column_object)
                id += 1

    def _generate(self):
        logging.info('Generating {} data'.format(self.benchmark_name))
        logging.info('scale factor: {}'.format(self.scale_factor))
        self._run_make()
        self._run_command(self.cmd)
        logging.info('[Generate command] ' + ' '.join(self.cmd))
        self._table_files()
        logging.info('Files generated: {}'.format(self.table_files))

    def create_database(self):
        self.db_connector.create_database(self.database_name())
        filename = self.directory + '/' + self.create_table_statements_file
        with open(filename, 'r') as file:
            data = file.read()
        # Do not create primary keys
        data = re.sub(r',\s*primary key (.*)', '', data)
        self.db_connector.db_name = self.database_name()
        self.db_connector.create_connection()
        self.db_connector.enable_simulation()
        logging.info('Creating tables')
        for create_statement in data.split(';')[:-1]:
            self.db_connector.exec_only(create_statement)
        self.db_connector.commit()
        self._load_table_data(self.db_connector)
        self.db_connector.close()

    def _load_table_data(self, database_connector):
        logging.info('Loading data into the tables')
        for filename in self.table_files:
            logging.debug('    Loading file {}'.format(filename))

            table = filename.replace('.tbl', '').replace('.dat', '')
            path = self.directory + '/' + filename
            size = os.path.getsize(path)
            logging.debug('    Import data of size {} b'.format(size))
            database_connector.import_data(table, path)
        database_connector.commit()

    def drop_database(self):
        self.db_connector.drop_database(self.database_name())

    def _run_make(self):
        if 'dbgen' not in self._files() and 'dsdgen' not in self._files():
            logging.info('Running make in {}'.format(self.directory))
            self._run_command(self.make_command)
        else:
            logging.info('No need to run make')

    def _table_files(self):
        self.table_files = [x for x in self._files()
                            if '.tbl' in x or '.dat' in x]

    def _run_command(self, command):
        cmd_out = '[SUBPROCESS OUTPUT] '
        p = subprocess.Popen(command, cwd=self.directory,
                             stdout=subprocess.PIPE,
                             stderr=subprocess.STDOUT)
        with p.stdout:
            for line in p.stdout:
                logging.info(cmd_out + line.decode('utf-8').replace('\n', ''))
        p.wait()

    def _files(self):
        return os.listdir(self.directory)

    def _prepare(self):
        if self.benchmark_name == 'tpch':
            self.make_command = ['make', 'DATABASE=POSTGRESQL']
            if platform.system() == 'Darwin':
                self.make_command.append('MACHINE=MACOS')

            self.directory = './tpch-kit/dbgen'
            self.create_table_statements_file = 'dss.ddl'
            self.cmd = ['./dbgen', '-s', str(self.scale_factor), '-f']
        elif self.benchmark_name == 'tpcds':
            self.make_command = ['make']
            if platform.system() == 'Darwin':
                self.make_command.append('OS=MACOS')

            self.directory = './tpcds-kit/tools'
            self.create_table_statements_file = 'tpcds.sql'
            self.cmd = ['./dsdgen', '-SCALE', str(self.scale_factor), '-FORCE']
            if int(self.scale_factor) - self.scale_factor != 0:
                raise Exception('Wrong TPCDS scale factor')
        else:
            raise NotImplementedError('only tpch implemented.')
