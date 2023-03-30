import itertools
import json
import logging
import time

from selection.index import Index
from selection.selection_algorithm import SelectionAlgorithm
from selection.workload import Column, Workload

DEFAULT_PARAMETERS = {}


class ILPActualResults(SelectionAlgorithm):
    def __init__(self, database_connector, parameters=None):
        if parameters is None:
            parameters = {}
        SelectionAlgorithm.__init__(
            self, database_connector, parameters, DEFAULT_PARAMETERS
        )
        self.budget = self.parameters["budget_MB"]
        self.input_file = self.parameters["input_file"]
        self.algorithm = self.parameters["algorithm"]
        self.benchmark_name = self.parameters["benchmark_name"]
        self.number_of_chunks = self.parameters["number_of_chunks"]

    def _calculate_best_indexes(self, workload):
        logging.info("Read ILP input data")
        logging.info("Parameters: " + str(self.parameters))

        with open(
            f"{self.input_file}"
        ) as f:
            data = json.loads(f.read())
            index_sizes = data['index_sizes']

            # Parse index and index size information
            indexes = {}
            for index in index_sizes:
                indexes[index["index_id"]] = {
                        "size": index["estimated_size"],
                        "index_columns": index["column_names"],
                    }

            # Parse combination information, i.e., the index subsets per combination
            index_combinations = {}
            for combination in data["index_combinations"]:
                    index_combinations[combination["combination_id"]] = combination["index_ids"]

        # Build a dictionary, which maps column names to objects
        columns = {}
        for query in workload.queries:
            for column in query.columns:
                columns[column.name] = column
        if self.benchmark_name == "tpch":
            # Index is created even if no query is there ..
            c = Column("s_acctbal")
            c.table = columns["s_suppkey"].table
            columns["s_acctbal"] = c

        # Parse result file for selected combinations
        if self.number_of_chunks == 1:
            with open(
                f"ILP/{self.benchmark_name}_{self.algorithm}_solution.txt"
            ) as f:
                result_str = f.read()
                for sub_result_str in result_str.split("\n\n\n")[1:-1]:
                    lines = sub_result_str.split("\n")
                    assert len(lines) == 6, f"{lines}"
                    storage_budget = int(float(lines[0].split(" = ")[-1]))
                    # relative_workload_costs = float(lines[1].split(" = ")[-1])
                    # ilp_time = float(lines[2].split(" = ")[-1])
                    print(storage_budget, self.budget)
                    if storage_budget == self.budget * 1000**2:
                        combination_ids = map(int, lines[5].split())
                        break
        else:
            with open(
                    f"ILP/{self.benchmark_name}_{self.algorithm}_budget{self.budget}_chunks{self.number_of_chunks}_solution.txt"
            ) as f:
                combination_ids = map(int, f.read().split("\n")[1].split(":")[1].split())

        # Generate set of selected index_ids based on combinations
        selected_index_ids = set()
        for c_id in combination_ids:
            for index_id in index_combinations[c_id]:
                selected_index_ids.add(index_id)

        solution = []
        selected_index_sizes = 0
        for index_id in selected_index_ids:
            selected_index_sizes += indexes[index_id]["size"]
            index_columns = []
            for index_column_str in indexes[index_id]["index_columns"]:
                index_columns.append(columns[index_column_str])
            index = Index(index_columns)
            solution.append(index)
        assert selected_index_sizes <= self.budget * 1000**2, f"{selected_index_sizes} not <= {self.budget * 1000**2}"
        print(selected_index_sizes)

        return solution
