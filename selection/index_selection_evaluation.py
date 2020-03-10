from .workload import Workload
from .workload import Query
from .workload import Column
from .workload import Table
from .benchmark import Benchmark
from .algorithms.microsoft_algorithm import MicrosoftAlgorithm
from .algorithms.drop_heuristic_algorithm import DropHeuristicAlgorithm
from .algorithms.epic_algorithm import EPICAlgorithm
from .algorithms.dexter_algorithm import DexterAlgorithm
from .algorithms.ibm_algorithm import IBMAlgorithm
from .dbms.postgres_dbms import PostgresDatabaseConnector
from .dbms.hana_dbms import HanaDatabaseConnector
#  from .index import Index
from .selection_algorithm import NoIndexAlgorithm, AllIndexesAlgorithm
from .table_generator import TableGenerator
from .query_generator import QueryGenerator

import logging
import json
import sys
import time
import copy
import os

ALGORITHMS = {
    'microsoft': MicrosoftAlgorithm,
    'drop_heuristic': DropHeuristicAlgorithm,
    'no_index': NoIndexAlgorithm,
    'all_indexes': AllIndexesAlgorithm,
    'ibm': IBMAlgorithm,
    'epic': EPICAlgorithm,
    'dexter': DexterAlgorithm
}

DBMSYSTEMS = {
    'postgres': PostgresDatabaseConnector,
    'hana': HanaDatabaseConnector
}


class IndexSelection:
    def __init__(self):
        logging.debug('Init IndexSelection')
        self.db_connector = None
        self.default_config_file = 'example_configs/config.json'
        self.disable_csv = False
        self.database_name = None
        self.database_system = None

    def run(self):
        """This is called when running `python3 -m selection`.
        """
        logging.getLogger().setLevel(logging.DEBUG)
        config_file = self._parse_command_line_args()
        if not config_file:
            config_file = self.default_config_file

        logging.info('Starting Index Selection Evaluation')
        logging.info('Using config file {}'.format(config_file))

        self._run_algorithms(config_file)

    def _setup_config(self, config):
        if config['benchmark_name'] == 'JOB':
            directory = "/home/Jan.Kossmann/index_selection_evaluation/join-order-benchmark"
            tables = []
            columns = []

            filename = f"{directory}/schema.sql"
            with open(filename, 'r') as file:
                data = file.read().lower()
            create_tables = data.split('create table ')[1:]
            for create_table in create_tables:
                splitted = create_table.split('(', 1)
                table = Table(splitted[0].strip())
                tables.append(table)
                # TODO regex split? ,[whitespace]\n
                for column in splitted[1].split(',\n'):
                    name = column.lstrip().split(' ', 1)[0]
                    if name == 'primary':
                        continue
                    column_object = Column(name)
                    table.add_column(column_object)
                    columns.append(column_object)

            queries = []
            for filename in os.listdir(directory):
                if '.sql' not in filename or 'fkindexes' in filename or 'schema' in filename:
                    continue
                query_id = filename.replace('.sql', '')

                with open(f"{directory}/{filename}", 'r') as file:
                    query_text = file.read()
                    query_text = query_text.replace('\t', '')
                    queries.append(Query(query_id, query_text))
                    assert "WHERE" in query_text
                    split = query_text.split('WHERE')
                    query_text_before_where = split[0]
                    query_text_after_where = split[1]
                    for column in columns:
                        if column.name in query_text_after_where and f"{column.table.name} " in query_text_before_where:
                            queries[-1].columns.append(column)
            dbms_class = DBMSYSTEMS[config['database_system']]
            self.database_name = 'indexselection_job___1'
            self.database_system = config['database_system']
            self.setup_db_connector(self.database_name, self.database_system)

            self.workload = Workload(queries, self.database_name)
        else:
            dbms_class = DBMSYSTEMS[config['database_system']]
            generating_connector = dbms_class(None, autocommit=True)
            table_generator = TableGenerator(config['benchmark_name'],
                                             config['scale_factor'],
                                             generating_connector)
            self.database_name = table_generator.database_name()
            self.database_system = config['database_system']
            self.setup_db_connector(self.database_name, self.database_system)
            if 'queries' not in config:
                config['queries'] = None

            query_generator = QueryGenerator(config['benchmark_name'],
                                             config['scale_factor'],
                                             self.db_connector, config['queries'],
                                             table_generator.columns)
            self.workload = Workload(query_generator.queries, self.database_name)

    def _run_algorithms(self, config_file):
        with open(config_file) as f:
            config = json.load(f)
        self._setup_config(config)
        self.db_connector.drop_indexes()
        self.db_connector.create_statistics()
        self.db_connector.commit()

        for algorithm_config in config['algorithms']:
            # There are multiple configs if there is a parameter list
            # configured (as a list in the .json file)
            configs = self._find_parameter_list(algorithm_config)
            for algorithm_config_unfolded in configs:
                start_time = time.time()
                cfg = algorithm_config_unfolded
                indexes, what_if, cost_requests, cache_hits, cache = self._run_algorithm(cfg)
                calculation_time = round(time.time() - start_time, 2)
                benchmark = Benchmark(self.workload, indexes,
                                      self.db_connector,
                                      algorithm_config_unfolded,
                                      calculation_time, self.disable_csv,
                                      config, cost_requests, cache_hits,
                                      what_if, cache)
                benchmark.benchmark()

    # Parameter list example: {"max_indexes": [5, 10, 20]}
    # Creates config for each value
    def _find_parameter_list(self, algorithm_config):
        parameters = algorithm_config['parameters']
        configs = []
        if parameters:
            # if more than one list --> raise
            self.__check_parameters(parameters)
            for key, value in parameters.items():
                if isinstance(value, list):
                    for i in value:
                        new_config = copy.deepcopy(algorithm_config)
                        new_config['parameters'][key] = i
                        configs.append(new_config)
        if len(configs) == 0:
            configs.append(algorithm_config)
        return configs

    def __check_parameters(self, parameters):
        counter = 0
        for key, value in parameters.items():
            if isinstance(value, list):
                counter += 1
        if counter > 1:
            raise Exception('Too many parameter lists in config')

    def _run_algorithm(self, config):
        self.db_connector.drop_indexes()
        self.db_connector.commit()
        self.setup_db_connector(self.database_name, self.database_system)


        algorithm = self.create_algorithm_object(config['name'],
                                                 config['parameters'])
        logging.info(f'Running algorithm {config}')
        indexes = algorithm.calculate_best_indexes(self.workload)
        logging.info('Indexes found: {}'.format(indexes))
        what_if = algorithm.cost_evaluation.what_if
        cost_requests = algorithm.cost_evaluation.cost_requests
        cache_hits = algorithm.cost_evaluation.cache_hits
        cache = algorithm.cost_evaluation.cache
        return indexes, what_if, cost_requests, cache_hits, cache

    def create_algorithm_object(self, algorithm_name, parameters):
        algorithm = ALGORITHMS[algorithm_name](self.db_connector, parameters)
        return algorithm

    def _parse_command_line_args(self):
        arguments = sys.argv
        if 'CRITICAL_LOG' in arguments:
            logging.getLogger().setLevel(logging.CRITICAL)
        if 'ERROR_LOG' in arguments:
            logging.getLogger().setLevel(logging.ERROR)
        if 'INFO_LOG' in arguments:
            logging.getLogger().setLevel(logging.INFO)
        if 'DISABLE_CSV' in arguments:
            self.disable_csv = True
        for argument in arguments:
            if '.json' in argument:
                return argument

    def setup_db_connector(self, database_name, database_system):
        if self.db_connector:
            logging.info('Create new database connector (closing old)')
            self.db_connector.close()
        self.db_connector = DBMSYSTEMS[database_system](database_name)
