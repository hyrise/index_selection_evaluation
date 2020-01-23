from .workload import Table, Column

import logging
import platform
import subprocess
import os
import re


class BaseTableGenerator:
    def __init__(self, database_connector, benchmark_name, scale_factor):
        self.scale_factor = scale_factor
        self.benchmark_name = benchmark_name
        self.db_connector = database_connector

        self.database_names = self.db_connector.database_names()
        self.tables = []
        self.columns = []
        self._prepare()
        if self.database_name() not in self.database_names:
            self._generate()
            self._table_files()
            logging.info('Files generated: {}'.format(self.table_files))

            self.create_database()
        else:
            logging.debug('Database with given scale factor already '
                          'existing')
        self._read_column_names()

    def _generate(self):
        raise NotImplementedError()

    def _get_table_file_path(self, filename):
        raise NotImplementedError()

    def _import_data(self, table, path):
        raise NotImplementedError

    def _cleanup_file(self, filename):
        raise NotImplementedError

    def _prepare(self):
        raise NotImplementedError

    def database_name(self):
        name = 'indexselection_' + self.benchmark_name + '___'
        name += str(self.scale_factor).replace('.', '_')
        return name

    def _read_column_names(self):
        # TODO is this needed?
        # Read table and column names from 'create table' statements
        id = 0
        with open(self.create_table_statements_file, 'r') as file:
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

    def create_database(self):
        self.db_connector.create_database(self.database_name())
        with open(self.create_table_statements_file, 'r') as file:
            create_statements = file.read()
        # Do not create primary keys
        create_statements = re.sub(r',\s*primary key (.*)', '',
                                   create_statements)
        self.db_connector.db_name = self.database_name()
        self.db_connector.create_connection()
        self.db_connector.create_tables(create_statements=create_statements)
        self._load_table_data()
        self.db_connector.enable_simulation()
        self.db_connector.close()

    def _load_table_data(self):
        logging.info('Loading data into the tables')
        for filename in self.table_files:
            logging.debug('    Loading file {}'.format(filename))

            table = filename.replace('.tbl', '').replace('.dat', '').replace('.csv', '')

            path = self._get_table_file_path(filename)

            size = os.path.getsize(path)
            size_string = '{:,} MB'.format(size / 1000000)
            logging.debug(f'    Import data of size {size_string}')

            self._import_data(table, path)
            self._cleanup_file(filename)
        self.db_connector.commit()

    def _table_files(self):
        self.table_files = [filename for filename in self._files()
                            if '.tbl' in filename or '.dat' in filename or '.csv' in filename]

    def _files(self):
        return os.listdir(self.directory)

class TableLoader(BaseTableGenerator):
    def __init__(self, database_connector, benchmark_name, create_table_file_directory, data_directory):
        self.create_table_statements_file = create_table_file_directory
        self.directory = data_directory

        BaseTableGenerator.__init__(self, database_connector, benchmark_name, 1)

    def _generate(self):
        # Data for these Benchmark is fix and existing, no generation necessary
        pass

    def _get_table_file_path(self, filename):
        return f"{self.directory}/{filename}"

    def _import_data(self, table, path):
        self.db_connector.import_data(table, path, delimiter=',', encoding='Latin-1')

    def _cleanup_file(self, filename):
        # No cleanup desired for user-provided data
        pass

    def _prepare(self):
        pass


class TableGenerator(BaseTableGenerator):
    def __init__(self, database_connector, benchmark_name, scale_factor):
        BaseTableGenerator.__init__(self, database_connector, benchmark_name, scale_factor)

    def _generate(self):
        logging.info('Generating {} data'.format(self.benchmark_name))
        logging.info('scale factor: {}'.format(self.scale_factor))
        self._run_make()
        self._run_command(self.cmd)
        if self.benchmark_name == 'tpcds':
            self._run_command(["bash", "../../scripts/replace_in_dat.sh"])
        logging.info('[Generate command] ' + ' '.join(self.cmd))

    def _prepare(self):
        if self.benchmark_name == 'tpch':
            self.make_command = ['make', 'DATABASE=POSTGRESQL']
            if platform.system() == 'Darwin':
                self.make_command.append('MACHINE=MACOS')

            self.directory = './tpch-kit/dbgen'
            self.create_table_statements_file = f"{self.directory}/dss.ddl"
            self.cmd = ['./dbgen', '-s', str(self.scale_factor), '-f']
        elif self.benchmark_name == 'tpcds':
            self.make_command = ['make']
            if platform.system() == 'Darwin':
                self.make_command.append('OS=MACOS')

            self.directory = './tpcds-kit/tools'
            self.create_table_statements_file = f"{self.directory}/tpcds.sql"
            self.cmd = ['./dsdgen', '-SCALE', str(self.scale_factor), '-FORCE']
            if int(self.scale_factor) - self.scale_factor != 0:
                raise Exception('Wrong TPCDS scale factor')
        else:
            raise NotImplementedError('Only TPCH/TPCDS table generation implemented.')

    def _run_make(self):
        if 'dbgen' not in self._files() and 'dsdgen' not in self._files():
            logging.info('Running make in {}'.format(self.directory))
            self._run_command(self.make_command)
        else:
            logging.info('No need to run make')

    def _run_command(self, command):
        cmd_out = '[SUBPROCESS OUTPUT] '
        p = subprocess.Popen(command, cwd=self.directory,
                             stdout=subprocess.PIPE,
                             stderr=subprocess.STDOUT)
        with p.stdout:
            for line in p.stdout:
                logging.info(cmd_out + line.decode('utf-8').replace('\n', ''))
        p.wait()

    def _get_table_file_path(self, filename):
        return f"{self.directory}/{filename}"

    def _import_data(self, table, path):
        self.db_connector.import_data(table, path)

    def _cleanup_file(self, filename):
        os.remove(os.path.join(self.directory, filename))
