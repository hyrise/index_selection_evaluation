"""
Module has function to read selection module output csvs and converts them into a dataclass for easier handling
"""

import ast
import csv
import json
import os
from pathlib import Path
from typing import Dict, List, Set

from benchmark_dataclass import BenchmarkDataclass


def convert_normal_row_to_dataclass(
    data_row: List[str],
    description: str,
    queries: List[str],
    plans_path: str,
) -> BenchmarkDataclass:
    """Converts a csv row into a BenchmarkDataclass"""
    data = BenchmarkDataclass(
        data_row[0],
        f'{data_row[2]}-{ast.literal_eval(data_row[3])["max_index_width"]}',
        ast.literal_eval(data_row[3]),
        data_row[5],
        data_row[4],
        data_row[6],
        data_row[2],
        convert_budget_to_mb(ast.literal_eval(data_row[3])),
        queries,
        convert_index(data_row[-1]),
        index_combination_extraction(data_row[0], plans_path),
        index_combination_extraction(
            data_row[0], plans_path
        ),  # Optimizer indexes == Algorithm indexes in this context
        calculate_overall_costs(retrieve_query_dicts(data_row)),
        convert_query_costs_list_to_dict(queries, retrieve_query_dicts(data_row)),
        float(data_row[7]),
        {"algorithm_time": data_row[6]},
        0,
        data_row[14],
        data_row[15],
        description=description,
    )
    return data


def extract_entries(
    path: Path, description: str, plans_path: str
) -> List[BenchmarkDataclass]:
    """Turns every non header row of a CSV into a Benchmark Data Object."""
    data_objects = []
    with open(path, newline="", encoding="utf-8") as file:
        reader = csv.reader(file, delimiter=";")
        queries = retrieve_query_names(next(reader))
        for row in reader:
            data_objects.append(
                convert_normal_row_to_dataclass(row, description, queries, plans_path)
            )
    return data_objects


def convert_budget_to_mb(config: Dict) -> int:
    """Converts a byte size to a megabyte size"""
    if "budget_MB" in config.keys():
        return int(config["budget_MB"]) * 1000 * 1000
    else:
        return int(config["budget"])


def convert_index(index_string: str) -> List[str]:
    """Removes brackets from an index for consistency and legibility"""
    # cuts off the brackets
    index_string = index_string[1:-1]
    return index_string.split(", ")


def retrieve_query_dicts(line: List) -> List[Dict]:
    """Retrieves the cost dict for all queries."""
    old = line[16:-1]
    new = []
    for item in old:
        new.append(ast.literal_eval(item))
    return new


def convert_query_costs_list_to_dict(queries: List[str], costs: List[Dict[str, str]]):
    """Turns a list of query cots into a dict of query costs with the query number as a key"""
    query_dict = {}
    for num, query in enumerate(queries):
        query_dict[query] = costs[num]
    return query_dict


def retrieve_query_names(row: List[str]) -> List[str]:
    """Gets the query names. Must be given the header row."""
    return row[16:-1]


def calculate_overall_costs(query_results: List[Dict]) -> int:
    """Combines the total costs from every query."""
    total = 0
    for costs in query_results:
        total += float(costs["Cost"])
    return total


def save_all_to_json(
    target_path: str, source_path: str, description: str, plans_path: str
):
    """Saves all files to json."""
    objects = extract_entries(source_path, description, plans_path)
    with open(target_path, "w+", encoding="utf-8") as file:
        file.write(json.dumps(objects))


def normalize_index_name(name: str) -> None:
    """Removes the id from the beginning of an index name so it can be compared."""
    cutoff = name.find(">")
    if not cutoff == -1:
        return name[cutoff + 1 :]
    return name


def rec_plan_search(node: Dict, indexes: Set[str]) -> None:
    """
    Checks if a node is an index, and if so adds it to the list of used indexes.
    Then does the same for all subnodes.
    node: the node to check
    indexes: the indexes set that is added to.
    """
    # None return type because it works in place

    if "Index" in node["Node Type"]:
        indexes.add(normalize_index_name(node["Index Name"]))

    if "Plans" in node.keys():
        for sub_node in node["Plans"]:
            rec_plan_search(sub_node, indexes)
    else:
        return


def index_combination_extraction(
    timestamp: str, plans_dir: str
) -> Dict[str, List[str]]:
    """
    Extracts the index combinations used from plans Json.
    timestamp: the timestamp associated with the run, this is the title of the target json.
    plans_dir: the directory where the plans can be found
    """

    filepath = f"{plans_dir}/{timestamp}.json"

    if not os.path.isfile(filepath):
        print("was not")
        return {}

    with open(filepath, "r", encoding="utf-8") as file:
        plans = json.load(file)
        indexes: Dict[str, Set[str]] = {}
        for query in plans.keys():
            indexes[f"q{query}"] = set()
            for node in plans[query]:
                rec_plan_search(node, indexes[f"q{query}"])
            indexes[f"q{query}"] = list(indexes[f"q{query}"])
    return indexes
