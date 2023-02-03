from pathlib import Path
from benchmark_dataclass import BenchmarkDataclass
from typing import List, Dict, Set
import ast
import csv
import json
import os


def convert_normal_row_to_dataclass(
    data_row: List[str],
    description: str,
    queries: List[str],
    plans_path: str,
) -> BenchmarkDataclass:
    data = BenchmarkDataclass(
        data_row[0],
        f'{data_row[2]}-{ast.literal_eval(data_row[3])["max_index_width"]}',
        ast.literal_eval(data_row[3]),
        data_row[5],
        data_row[4],
        data_row[6],
        data_row[2],
        convert_budget(ast.literal_eval(data_row[3])),
        queries,
        convert_index(data_row[-1]),
        combi_extract(data_row[0], plans_path),
        {},
        calculate_overall_costs(retrieve_query_dicts(data_row)),
        convert_query_costs_list_to_dict(queries, retrieve_query_dicts(data_row)),
        data_row[7],
        {"algorithm_time": data_row[6]},
        0,
        0,
        0,
        description=description,
    )  # TODO What if time, what if cache hits, algoirthm indexes, optimizer indexes
    return data


def convert_budget(config: Dict) -> int:
    if "budget_MB" in config.keys():
        return int(config["budget_MB"]) * 1000 * 1000
    else:
        return int(config["budget"])


def convert_index(index_string: str) -> List[str]:
    # cuts off the brackets
    index_string = index_string[1:-1]
    return index_string.split(", ")


def retrieve_query_dicts(line: List) -> List[Dict]:
    old = line[16:-1]
    new = []
    for item in old:
        new.append(ast.literal_eval(item))
    return new

def convert_query_costs_list_to_dict(queries: List[str], costs: List[Dict[str, str]]):
    dict = {}
    for num, query in enumerate(queries):
        dict[query] = costs[num]
    return dict


def retrieve_query_names(row: List[str]) -> List[str]:
    return row[16:-1]


def calculate_overall_costs(query_results: List[Dict]) -> int:
    total = 0
    for costs in query_results:
        total += float(costs["Cost"])
    return total


def extract_entries(path: Path, description: str, plans_path: str) -> List:
    data_objects = []
    with open(path, newline="") as file:
        reader = csv.reader(file, delimiter=";")
        queries = retrieve_query_names(reader.__next__())
        for row in reader:
            data_objects.append(
                convert_normal_row_to_dataclass(row, description, queries, plans_path)
            )
    return data_objects


def save_all_to_json(target_path: str, source_path: str, description: str):
    # TODO test
    objects = extract_entries(source_path, description)

    with open(target_path, "w+") as file:
        file.write(json.dumps(objects))


def rec_plan_search(node: Dict, indexes: Set):
    if "index" in node["Node Type"]:
        indexes.add(node["Index Name"])

    if "Plans" in node.keys():
        for sub_node in node["Plans"]:
            rec_plan_search(sub_node, indexes)
    else:
        return


def combi_extract(timestamp: str, plans_dir: str) -> Dict[str, List[str]]:

    filepath = f"{plans_dir}/{timestamp}.json"

    if not os.path.isfile(filepath):
        print('was not')
        return {}

    with open(filepath, "r") as file:
        plans = json.load(file)
        indexes: Dict[str, Set[str]] = {}
        for query in plans.keys():
            indexes[query] = set()
            for node in plans[query]:
                rec_plan_search(node, indexes[query])
    return indexes


def noindex_costs(path):
    # Hacky but leass annoying than the alternative
    with open(path, "r") as file:
        reader = csv.reader(file, delimiter=";")
        reader.__next__()
        return calculate_overall_costs(retrieve_query_dicts(reader.__next__()))
