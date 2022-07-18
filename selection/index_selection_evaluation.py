import copy
import json
import logging
import pickle
import sys
import time

from .algorithms.anytime_algorithm import AnytimeAlgorithm
from .algorithms.auto_admin_algorithm import AutoAdminAlgorithm
from .algorithms.db2advis_algorithm import DB2AdvisAlgorithm
from .algorithms.dexter_algorithm import DexterAlgorithm
from .algorithms.drop_heuristic_algorithm import DropHeuristicAlgorithm
from .algorithms.extend_algorithm import ExtendAlgorithm
from .algorithms.relaxation_algorithm import RelaxationAlgorithm
from .benchmark import Benchmark
from .dbms.hana_dbms import HanaDatabaseConnector
from .dbms.postgres_dbms import PostgresDatabaseConnector
from .query_generator import QueryGenerator
from .selection_algorithm import AllIndexesAlgorithm, NoIndexAlgorithm
from .table_generator import TableGenerator
from .workload import Workload

ALGORITHMS = {
    "anytime": AnytimeAlgorithm,
    "auto_admin": AutoAdminAlgorithm,
    "db2advis": DB2AdvisAlgorithm,
    "dexter": DexterAlgorithm,
    "drop": DropHeuristicAlgorithm,
    "extend": ExtendAlgorithm,
    "relaxation": RelaxationAlgorithm,
    "no_index": NoIndexAlgorithm,
    "all_indexes": AllIndexesAlgorithm,
}

DBMSYSTEMS = {"postgres": PostgresDatabaseConnector, "hana": HanaDatabaseConnector}


class IndexSelection:
    def __init__(self):
        logging.debug("Init IndexSelection")
        self.db_connector = None
        self.default_config_file = "example_configs/config_tpch.json"
        self.disable_output_files = False
        self.database_name = None
        self.database_system = None

    def run(self):
        """This is called when running `python3 -m selection`."""
        logging.getLogger().setLevel(logging.DEBUG)
        config_file = self._parse_command_line_args()
        if not config_file:
            config_file = self.default_config_file

        logging.info("Starting Index Selection Evaluation")
        logging.info("Using config file {}".format(config_file))

        self._run_algorithms(config_file)

    def _setup_config(self, config):
        dbms_class = DBMSYSTEMS[config["database_system"]]
        generating_connector = dbms_class(None, autocommit=True)
        table_generator = TableGenerator(
            config["benchmark_name"], config["scale_factor"], generating_connector
        )
        self.database_name = table_generator.database_name()
        self.database_system = config["database_system"]
        self.setup_db_connector(self.database_name, self.database_system)

        if "queries" not in config:
            config["queries"] = None
        query_generator = QueryGenerator(
            config["benchmark_name"],
            config["scale_factor"],
            self.db_connector,
            config["queries"],
            table_generator.columns,
        )
        self.workload = Workload(query_generator.queries)

        if "pickle_workload" in config and config["pickle_workload"] is True:
            pickle_filename = (
                f"benchmark_results/workload_{config['benchmark_name']}"
                f"_{len(self.workload.queries)}_queries.pickle"
            )
            pickle.dump(self.workload, open(pickle_filename, "wb"))

    def _run_algorithms(self, config_file):
        with open(config_file) as f:
            config = json.load(f)
        self._setup_config(config)
        self.db_connector.drop_indexes()

        # Set the random seed to obtain deterministic statistics (and cost estimations)
        # because ANALYZE (and alike) use sampling for large tables
        self.db_connector.create_statistics()
        self.db_connector.commit()

        for algorithm_config in config["algorithms"]:
            # CoPhy must be skipped and manually executed via AMPL because it is not
            # integrated yet.
            if algorithm_config["name"] == "cophy":
                continue

            # There are multiple configs if there is a parameter list
            # configured (as a list in the .json file)
            configs = self._find_parameter_list(algorithm_config)
            for algorithm_config_unfolded in configs:
                start_time = time.time()
                cfg = algorithm_config_unfolded
                indexes, what_if, cost_requests, cache_hits = self._run_algorithm(cfg)
                calculation_time = round(time.time() - start_time, 2)
                benchmark = Benchmark(
                    self.workload,
                    indexes,
                    self.db_connector,
                    algorithm_config_unfolded,
                    calculation_time,
                    self.disable_output_files,
                    config,
                    cost_requests,
                    cache_hits,
                    what_if,
                )
                benchmark.benchmark()

    # Parameter list example: {"max_indexes": [5, 10, 20]}
    # Creates config for each value
    def _find_parameter_list(self, algorithm_config):
        parameters = algorithm_config["parameters"]
        configs = []
        if parameters:
            # if more than one list --> raise
            self.__check_parameters(parameters)
            for key, value in parameters.items():
                if isinstance(value, list):
                    for i in value:
                        new_config = copy.deepcopy(algorithm_config)
                        new_config["parameters"][key] = i
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
            raise Exception("Too many parameter lists in config")

    def _run_algorithm(self, config):
        self.db_connector.drop_indexes()
        self.db_connector.commit()
        self.setup_db_connector(self.database_name, self.database_system)

        algorithm = self.create_algorithm_object(config["name"], config["parameters"])
        logging.info(f"Running algorithm {config}")
        indexes = algorithm.calculate_best_indexes(self.workload)
        logging.info(f"Indexes found: {indexes}")
        what_if = algorithm.cost_evaluation.what_if

        cost_requests = (
            self.db_connector.cost_estimations
            if config["name"] == "db2advis"
            else algorithm.cost_evaluation.cost_requests
        )
        cache_hits = (
            0 if config["name"] == "db2advis" else algorithm.cost_evaluation.cache_hits
        )
        return indexes, what_if, cost_requests, cache_hits

    def create_algorithm_object(self, algorithm_name, parameters):
        algorithm = ALGORITHMS[algorithm_name](self.db_connector, parameters)
        return algorithm

    def _parse_command_line_args(self):
        arguments = sys.argv
        if "CRITICAL_LOG" in arguments:
            logging.getLogger().setLevel(logging.CRITICAL)
        if "ERROR_LOG" in arguments:
            logging.getLogger().setLevel(logging.ERROR)
        if "INFO_LOG" in arguments:
            logging.getLogger().setLevel(logging.INFO)
        if "DISABLE_OUTPUT_FILES" in arguments:
            self.disable_output_files = True
        for argument in arguments:
            if ".json" in argument:
                return argument

    def setup_db_connector(self, database_name, database_system):
        if self.db_connector:
            logging.info("Create new database connector (closing old)")
            self.db_connector.close()
        self.db_connector = DBMSYSTEMS[database_system](database_name)
