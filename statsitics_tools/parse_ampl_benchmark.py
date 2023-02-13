import csv
import json
from typing import Dict, List, Tuple, Union
import ast
import os

from benchmark_dataclass import BenchmarkDataclass
from parse_normal_benchmark import convert_budget_to_mb, retrieve_query_names


def extract_cophy_entries(
    path: str, description: str, budgets: List[int]
) -> List[BenchmarkDataclass]:
    """
    extracts all rows from a cophy csv into Benchmark Dataclasses
    path: The CSVs path.
    description: The description string for this run.
    """
    data_objects = []
    with open(path, newline="", encoding="utf-8") as file:
        reader = csv.reader(file, delimiter=";")
        queries = retrieve_query_names(next(reader))
        for row in reader:
            data_objects = data_objects + convert_cophy_row(
                row, description, queries, budgets
            )
    return data_objects


def convert_cophy_row(
    row: List[str], description: str, queries: List[str], budgets: List[int]
) -> List[BenchmarkDataclass]:
    data_classes = []
    config = ast.literal_eval(row[3])
    for budget in budgets:
        solve_path = make_solve_path(config, budget)
        if not os.path.isfile(solve_path):
            print(f"No Solve for {solve_path}")
            continue
        solve_dict = parse_solve(solve_path, make_json_path(config))
        data = BenchmarkDataclass(
            row[0],
            f'{row[2]}-{config["max_index_width"]}-{config["max_indexes_per_query"]}',
            config,
            row[5],
            row[4],
            row[6],
            row[2],
            budget,
            queries,
            solve_dict["selected_indexes"],
            solve_dict["algorithm_indexes_by_query"],
            parse_optimizer_run(),  # Parse,
            solve_dict["overall_costs"],
            solve_dict["costs_by_query"],
            float(row[7]) + solve_dict["time_run_solve"],
            {
                "generate_time": float(row[7]),
                "solve_time": solve_dict["time_run_solve"],
            },
            0,
            row[14],
            row[15],
            description=description,
        )
        data_classes.append(data)
    return data_classes


def make_solve_path(config: Dict[str, str], budget: int):
    return f'solves/{config["benchmark_name"]}_cophy_input__width{config["max_index_width"]}__per_query{config["max_indexes_per_query"]}.txt-{budget}-out.solve'


def make_json_path(config: dict[str, str]):
    return f'{config["output_folder"]}/{config["benchmark_name"]}_cophy_input__width{config["max_index_width"]}__per_query{config["max_indexes_per_query"]}.json'


def parse_solve(solve_file_path: str, json_path: str) -> Dict[str, str]:
    """parses a solve file"""

    data_dict = json.load(open(json_path, encoding="utf-8"))
    index_dict = transform_index_list(data_dict["index_sizes"])
    combination_dict = transform_combination_list(data_dict["index_combinations"])
    costs_dict = transform_query_costs(data_dict["query_costs"])
    with open(solve_file_path, "r", encoding="utf-8") as file:
        solve_parse_dict = {}
        line = file.readline()

        objective_index = line.find("objective")
        if objective_index == -1:
            raise (f"Solve file {solve_file_path} is not well formed")

        solve_parse_dict["overall_costs"] = float(line[objective_index + 10 :])

        solve_parse_dict["selected_indexes"] = []
        # seek to x
        while not "x [*] :=" in line:
            line = file.readline()

        # skip assignment line
        line = file.readline().strip()

        while not ";" in line:
            contents = line.split()
            if contents[1] == "1":
                solve_parse_dict["selected_indexes"] += index_dict[contents[0]][
                    "column_names"
                ]
            line = file.readline().strip()

        solve_parse_dict["algorithm_indexes_by_query"] = {}
        solve_parse_dict["costs_by_query"] = {}
        # seek to Z
        while not "z :=" in line:
            line = file.readline()
        # skip assignment line
        line = file.readline().strip()

        while not ";" in line:
            contents = line.split()
            if contents[2] == "1":
                solve_parse_dict["algorithm_indexes_by_query"][
                    contents[1]
                ] = convert_index_ids_to_names(
                    combination_dict[contents[0]], index_dict
                )
                solve_parse_dict["costs_by_query"][contents[1]] = costs_dict[
                    contents[1]
                ][contents[0]]
            line = file.readline().strip()

        while not "Time" in line:
            line = file.readline().strip()

        solve_parse_dict["time_run_solve"] = float(line[6:])

    return solve_parse_dict


def transform_index_list(indexes: List[Dict[str, List[str]]]) -> Dict[str, str]:
    """Transforms the index sizes list into a dictionary"""
    index_dict = {}
    for index_info in indexes:
        index_dict[str(index_info["index_id"])] = index_info

    return index_dict


def transform_combination_list(combinations: List[Dict[str, str]]):
    """Transform the list of combinations into a dictionary"""
    combinations_dict = {}

    for item in combinations:
        combinations_dict[str(item["combination_id"])] = item["index_ids"].split()

    return combinations_dict


def transform_query_costs(
    query_costs: List[Dict[str, Union[float, int]]]
) -> Dict[str, Dict[str, float]]:
    costs_dict = {}

    for cost_info in query_costs:
        query_number = str(cost_info["query_number"])
        combination_id = str(cost_info["combination_id"])
        if query_number not in costs_dict:
            costs_dict[query_number] = {}
        # This is because thats how it looks in the csv. No I dont like it either.
        costs_dict[query_number][combination_id] = {"Cost": cost_info["costs"]}

    return costs_dict


def convert_index_ids_to_names(
    ids: List[str], index_dict: Dict[str, List[str]]
) -> List[str]:
    "convert a list of index ids to their name."
    combined_list = []
    for id in ids:
        for index in index_dict[id]["column_names"]:
            combined_list.append(index)
    return combined_list


def parse_optimizer_run() -> Dict[str, str]:
    return None
