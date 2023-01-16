from pathlib import Path
from run_dataclass import BenchmarkDataclass
from typing import List, Dict
import ast
import csv

def convert_normal_row_to_dataclass(
    data_row: List[str],
    sequence: str,
    description: str,
    queries: List[str],
) -> BenchmarkDataclass:
    data = BenchmarkDataclass(
        data_row[0],
        sequence,
        ast.literal_eval(data_row[3]),
        data_row[5],
        data_row[4],
        data_row[6],
        data_row[2],
        convert_budget(ast.literal_eval(data_row[3])),
        queries,
        convert_index(data_row[-1]),
        {},
        {},
        calculate_overall_costs(retrieve_query_dicts(data_row)),
        retrieve_query_dicts(data_row),
        data_row[7],
        {"algorithm_time": data_row[6]},
        0,
        0,
        0,
        description=description,
    ) # TODO What if time, what if cache hits, algoirthm indexes, optimizer indexes
    return data


def convert_budget(config: Dict) -> int:
    if "budget_MB" in config.keys():
        return int(config["budget_MB"]) * 1000 * 1000
    else:
        return int(config["budget"])

def convert_index(index_string: str) -> List[str]:
    # cuts off the brackets
    index_string = index_string[1:-1]
    return index_string.split(', ')

def retrieve_query_dicts(line: List) -> List[Dict]:
    old = line[16:-1]
    new = []
    for item in old:
        new.append(ast.literal_eval(item))
    return new

def retrieve_query_names(row: List[str]) -> List[str]:
    return row[16:-1]

def calculate_overall_costs(query_results: List[Dict]) -> int:
    total = 0
    for costs in query_results:
        total += float(costs["Cost"])
    return total

def extract_entries(path: Path, sequence: str, description: str) -> List:
    data_objects = []
    with open(path, newline = '') as file:
        reader = csv.reader(file, delimiter=';')
        queries = retrieve_query_names(reader.__next__())
        print(queries)
        for row in reader:
            data_objects.append(convert_normal_row_to_dataclass(
                row,
                sequence,
                description,
                queries
            ))
    return data_objects
