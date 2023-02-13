from typing import List
import csv

from parse_normal_benchmark import (
    extract_entries,
    calculate_overall_costs,
    retrieve_query_dicts,
)
from benchmark_dataclass import BenchmarkDataclass
from parse_ampl_benchmark import extract_cophy_entries


def convert_normal_csvs(
    data_paths: List[str], plans_path: str
) -> List[BenchmarkDataclass]:
    """
    This gets a list of normal CSV files for selection
    benchmarks and turns them into Benchmark Dataclasses
    """
    data: List[BenchmarkDataclass] = []

    for item in data_paths:
        data += extract_entries(item, "for plotting", plans_path)
    return data


def convert_cophy_csvs(
    data_paths: List[str], budgets: List[int]
) -> List[BenchmarkDataclass]:
    """
    This gets COPHY style CSVs and converts them into benchmar Dataclasses
    """
    data: List[BenchmarkDataclass] = []

    for item in data_paths:
        data += extract_cophy_entries(item, "for_plotting", budgets)
    return data


def no_index_costs(path):
    """Gets the costs for a no index run."""
    # Hacky but less annoying than the alternative
    with open(path, "r", encoding="utf-8") as file:
        reader = csv.reader(file, delimiter=";")
        reader.next()
        return calculate_overall_costs(retrieve_query_dicts(reader.next()))
