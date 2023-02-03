from typing import Dict, List
from benchmark_dataclass import BenchmarkDataclass
from read_csv import extract_entries


def overall_costs_breakdown(data: List[BenchmarkDataclass]):

    budgets = {}

    for item in data:
        if item.budget_in_bytes not in budgets.keys():
            budgets[item.budget_in_bytes] = {}
        if item.overall_costs not in budgets[item.budget_in_bytes].keys():
            budgets[item.budget_in_bytes][item.overall_costs] = []
        budgets[item.budget_in_bytes][item.overall_costs].append(item.sequence)

    return budgets


def equal_index_configs_by_budget(data):
    budgets = {}
    for item in data:
        if item.budget_in_bytes not in budgets.keys():
            budgets[item.budget_in_bytes] = {}
        sorted_indexes = str(sorted(item.selected_indexes))
        if sorted_indexes not in budgets[item.budget_in_bytes].keys():
            budgets[item.budget_in_bytes][sorted_indexes] = []
        budgets[item.budget_in_bytes][sorted_indexes].append(item.sequence)

    return budgets


def indexes_by_budget(data: List[BenchmarkDataclass]):
    budgets = {}
    for item in data:
        if item.budget_in_bytes not in budgets.keys():
            budgets[item.budget_in_bytes] = {}
        for index in item.selected_indexes:
            if index not in budgets[item.budget_in_bytes].keys():
                budgets[item.budget_in_bytes][index] = []
            budgets[item.budget_in_bytes][index].append(item.sequence)

    return budgets


def costs_by_query(data: List[BenchmarkDataclass]):
    budgets = {}
    for item in data:
        if item.budget_in_bytes not in budgets.keys():
            budgets[item.budget_in_bytes] = {}
        for query_id in item.costs_by_query:
            if query_id not in budgets[item.budget_in_bytes].keys():
                budgets[item.budget_in_bytes][query_id] = {}
            print(query_id)
            budgets[item.budget_in_bytes][query_id].update(
                {item.sequence: item.costs_by_query[query_id]["Cost"]}
            )
    return budgets


def compare_indexes(
    baseline_indexes: List[str], other_indexes: List[List[str]]
) -> Dict[str, List[str]]:

    baseline_set = set(baseline_indexes)


def compare_algorithm_costs(
    data: List[BenchmarkDataclass],
    budget: str,
    baseline_ident: str,
    compare_algorithms: List[str],
):

    # Todo: COmpare alogrithms of different budgets
    base: BenchmarkDataclass = None
    compare: List[BenchmarkDataclass] = []

    for item in data:
        if item.sequence == baseline_ident and item.budget_in_bytes == budget:
            base = item
        if item.sequence in compare_algorithms and item.budget_in_bytes == budget:
            compare.append(item)

    if not base:
        print("Compare not found")
        return
    if len(compare) < 1:
        print("nothing to compare too")
        return

    out_dict = {}
    out_dict["totals"] = [f"{base.sequence} : {base.overall_costs}"]
    out_dict["total_index_configs"] = {"Base_indexes": base.selected_indexes}

    out_dict['queries'] = {}
    for q in base.costs_by_query:
        out_dict['queries'].update({q: {'base': base.costs_by_query[q]['Cost']}})

    for item in compare:
        out_dict["totals"].append(
            f"{item.sequence} : {item.overall_costs} ({item.overall_costs - base.overall_costs})"
        )
        base_set = set(base.selected_indexes)
        compare_set = set(item.selected_indexes)
        out_dict["total_index_configs"].update(
            {
                item.sequence: {
                    "shared": list(base_set.intersection(compare_set)),
                    "compare_only": list(compare_set.difference(base_set)),
                    "base_only": list(base_set.difference(compare_set)),
                }
            }
        )
        for query in item.costs_by_query:
            out_dict['queries'][query][item.sequence] = {'cost': item.costs_by_query[query]["Cost"], 'Difference': item.costs_by_query[query]["Cost"] -  base.costs_by_query[query]["Cost"]}


    return out_dict


def combine_data_files(
    data_paths: List[str], plans_path: str
) -> List[BenchmarkDataclass]:
    data: List[BenchmarkDataclass] = []

    for item in data_paths:
        data += extract_entries(item, "for plotting", plans_path)
    return data
