import time
import json
import random
import logging
import datetime
import subprocess
import os.path


class Benchmark:
    def __init__(self, workload, indexes, db_connector, config,
                 calculation_time, disable_csv, global_config,
                 parameter_range=False, what_if=None):
        self.workload = workload
        self.db_connector = db_connector
        self.indexes = indexes
        self.timeout = config['timeout']
        self.number_of_runs = config['number_of_runs']
        self.config = config
        self.calculation_time = calculation_time
        self.disable_csv = disable_csv
        self.parameter_range = parameter_range
        self._set_csv_filename(disable_csv)
        self.what_if = what_if

        self.scale_factor = global_config['scale_factor']
        self.benchmark_name = global_config['benchmark_name']
        self.db_system = global_config['database_system']
        self.seed = None
        if 'seed' in global_config:
            self.seed = global_config['seed']

    def benchmark(self):
        self.db_connector.drop_indexes()

        logging.info('Benchmark with config: {}'.format(self.config))
        if self.number_of_runs > 0:
            self._create_indexes()
        else:
            self.index_create_time = 0
            for index in self.indexes:
                self.what_if.simulate_index(index)
        self.db_connector.create_statistics()
        self._benchmark()
        if self.number_of_runs > 0:
            self._drop_indexes()
        else:
            self.what_if.drop_all_simulated_indexes()

    def _create_csv_header(self):
        header = ['date', 'commit', 'algorithm name', 'parameters',
                  'scale factor', 'benchmark name', 'db system',
                  'algorithm runtime', '#indexes', 'index create time',
                  'memory consumption']
        for query in self.workload.queries:
            header.append('q' + str(query.nr))
        header.append('indexed columns')
        return ';'.join(header)

    def _git_hash(self):
        githash = subprocess.check_output(['git',
                                           'rev-parse', '--short', 'HEAD'])
        return githash.decode('ascii').replace('\n', '')

    def _store_results(self, results, plans):
        config = self.config
        date = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        if not self.disable_csv:
            self._write_query_plans(date, plans)
        commit_hash = self._git_hash()
        indexes_size = self.db_connector.indexes_size()
        csv_entry = [date, commit_hash, config['name'], config['parameters'],
                     self.scale_factor, self.benchmark_name, self.db_system,
                     self.calculation_time, len(self.indexes),
                     self.index_create_time, indexes_size]
        csv_entry.extend(results)
        csv_entry.append(sorted(self.indexes))
        self._append_to_csv(';'.join([str(x) for x in csv_entry]))

    def _write_query_plans(self, date, plans):
        with open(f'benchmark_results/plans/{date}.json', 'w') as f:
            json.dump(plans, f)

    def _append_to_csv(self, entry):
        header = self._create_csv_header()
        entry = entry.replace("'", '"').replace('None', 'null')
        if not os.path.isfile(self.filename):
            entry = header + '\n' + entry
        with open(self.filename, 'a') as f:
            f.write(entry + '\n')
        logging.info(f'Results written to {self.filename}')

    def _benchmark(self):
        logging.info('Benchmark all queries')
        results = [{'Runtimes': [], 'Hits': []}
                   for x in self.workload.queries]
        plans = {x.nr: [] for x in self.workload.queries}

        for query_id in range(len(self.workload.queries)):
            query = self.workload.queries[query_id]
            cost = self.db_connector.get_cost(query)
            results[query_id]['Cost'] = cost
        for i in range(self.number_of_runs):
            logging.debug('Benchmark Run {}'.format(i))
            random_query_indexes = list(range(len(self.workload.queries)))
            seed = time.time()
            if self.seed:
                seed = self.seed
            logging.debug(f'Random seed: {seed}')
            random.seed(seed)
            random.shuffle(random_query_indexes)
            for query_index in random_query_indexes:
                query = self.workload.queries[query_index]
                logging.debug('Run {}'.format(query))
                execution_time, plan = self._benchmark_query(query)
                results[query_index]['Runtimes'].append(execution_time)
                results[query_index]['Hits'].append(self._calculate_hits(plan))
                plans[query.nr].append(plan)
        logging.debug('Execution times: {}'.format(results))
        self._store_results(results, plans)

    def _benchmark_query(self, query):
        exec_result = self.db_connector.exec_query(query,
                                                   timeout=self.timeout)
        return exec_result

    def _calculate_hits(self, plan):
        ratio = None
        if 'Shared Hit Blocks' in plan:
            hits = plan['Shared Hit Blocks']
            reads = plan['Shared Read Blocks']
            ratio = hits / (hits + reads)
        return ratio

    def _create_indexes(self):
        logging.info('Creating the indexes')
        start_time = time.time()
        for index in self.indexes:
            logging.debug('create index on {}'.format(index))
            self.db_connector.create_index(index)
        self.index_create_time = round(time.time() - start_time, 2)

    def _drop_indexes(self):
        for index in self.indexes:
            self.db_connector.drop_index(index)
        self.db_connector.commit()

    def _set_csv_filename(self, disable_csv):
        name = 'results_{}_queries.csv'.format(len(self.workload.queries))
        self.filename = 'benchmark_results/' + name
        if self.parameter_range:
            self.filename = 'benchmark_results/range_' + name
        if disable_csv:
            self.filename = os.devnull
