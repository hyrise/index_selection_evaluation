from typing import Dict, List
from benchmark_dataclass import BenchmarkDataclass


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

    out_dict["queries"] = {}
    for q in base.costs_by_query:
        out_dict["queries"].update({q: {"base": base.costs_by_query[q]["Cost"]}})

    base_selected_indexes_set = set(base.selected_indexes)
    for item in compare:
        out_dict["totals"].append(
            f"{item.sequence} : {item.overall_costs} ({item.overall_costs - base.overall_costs})"
        )
        compare_selected_indexes_set = set(item.selected_indexes)
        out_dict["total_index_configs"].update(
            {
                item.sequence: {
                    "shared": list(
                        base_selected_indexes_set.intersection(
                            compare_selected_indexes_set
                        )
                    ),
                    "compare_only": list(
                        compare_selected_indexes_set.difference(
                            base_selected_indexes_set
                        )
                    ),
                    "base_only": list(
                        base_selected_indexes_set.difference(
                            compare_selected_indexes_set
                        )
                    ),
                }
            }
        )
        for query in item.costs_by_query:
            base_query_set = set(item.algorithm_indexes_by_query)  # TODO rename
            compare_query_set = set(
                item.algorithm_indexes_by_query[query]
            )  # TODO rename
            out_dict["queries"][query][item.sequence] = {
                "cost": item.costs_by_query[query]["Cost"],
                "Difference": item.costs_by_query[query]["Cost"]
                - base.costs_by_query[query]["Cost"],
                "shared": list(base_query_set.intersection(compare_query_set)),
                "compare_only": list(compare_query_set.difference(base_query_set)),
                "base_only": list(base_query_set.difference(base_query_set)),
            }
    return out_dict


def compare_absolute_differences(comparisons_dict: dict) -> list[str]:
    difference_list = []
    for query, value in comparisons_dict["queries"].items():
        for algorithm, costs in value.items():
            if algorithm == "base":
                continue
            difference_list.append(
                {
                    "query": query,
                    "algorithm": algorithm,
                    "difference": costs["Difference"],
                    "absolute_difference": abs(costs["Difference"]),
                    "percentage_total": abs(costs["Difference"])
                    / 79382931.60999997,  # TODO CRITICAL THIS CANNOT BE HARDCODED!!!!!!!
                }
            )

    return sorted(difference_list, reverse=True, key=lambda x: x["absolute_difference"])
