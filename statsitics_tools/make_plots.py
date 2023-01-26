from typing import Dict, List
from read_csv import extract_entries
from benchmark_dataclass import BenchmarkDataclass
import matplotlib.pyplot as plt
import json
from plot_helper import PlotHelper
import numpy as np


def plot_runtime(data: List[BenchmarkDataclass], plot_helper: PlotHelper):
    config_run_times = {}
    config_budgets = {}

    for item in data:
        if item.sequence not in config_run_times.keys():
            config_run_times[item.sequence] = []
            config_budgets[item.sequence] = []

        config_run_times[item.sequence].append(item.time_run_total)
        config_budgets[item.sequence].append(item.budget_in_bytes / 1000**2)

    for algorithm in config_budgets.keys():
        plt.step(
            config_budgets[algorithm],
            config_run_times[algorithm],
            where="mid",
            label=algorithm,
            color=plot_helper.get_color(algorithm),
            marker=plot_helper.get_symbol(algorithm),
            fillstyle="none",
        )

    plt.xlabel("Budget")
    plt.ylabel("Runtime Algorithm")
    plt.title(f"Algortihm Runtime on {data[0].benchmark}")
    plt.legend()
    plt.show()


def plot_overall_costs(
    data: List[BenchmarkDataclass],
    removes: List[str],
    no_index_cost: float,
    plot_helper: PlotHelper,
):

    config_overall_costs: Dict[str, List[float]] = {}
    config_budgets: Dict[str, List[float]] = {}

    for data_object in data:
        if data_object.sequence in removes:
            continue
        if data_object.sequence not in config_overall_costs.keys():
            config_overall_costs[data_object.sequence] = []
            config_budgets[data_object.sequence] = []

        config_overall_costs[data_object.sequence].append(
            data_object.overall_costs / no_index_cost
        )
        config_budgets[data_object.sequence].append(
            data_object.budget_in_bytes / 1000**2
        )

    for algorithm in config_budgets.keys():
        plt.step(
            config_budgets[algorithm],
            config_overall_costs[algorithm],
            where="mid",
            label=algorithm,
            marker=plot_helper.get_symbol(algorithm),
            color=plot_helper.get_color(algorithm),
            fillstyle="none",
        )

    plt.xlabel("Budget")
    plt.ylabel("Costs Algorithm")
    plt.title(f"Algortihm Total costs on {data[0].benchmark}")
    plt.legend()
    plt.show()


def plot_costs_by_query_individual(
    dict: Dict[str, Dict[str, Dict[str, str]]],
    budgets: List[int],
    plot_helper: PlotHelper,
):

    for budget in budgets:
        for query in dict[budget]:
            algorithms = []
            costs = []
            colors = []
            for algorithm in dict[budget][query]:
                algorithms.append(algorithm)
                costs.append(dict[budget][query][algorithm])
                colors.append(plot_helper.get_color(algorithm))
            plt.bar(algorithms, costs, color=colors)
            plt.xlabel("Algorithm")
            plt.ylabel("Cost")
            plt.title(f"Costs for {query}")
            plt.show()


def plot_costs_by_query_combined(
    dict: Dict[str, Dict[str, Dict[str, str]]], budget: int, plot_helper: PlotHelper
):
    data = dict[budget]
    size = len(data)
    algorithm_dict = {}
    queries = []
    for query_name in data:
        queries.append(query_name)
        for algorithm_name in data[query_name]:
            if algorithm_name not in algorithm_dict:
                algorithm_dict[algorithm_name] = []
            algorithm_dict[algorithm_name].append(data[query_name][algorithm_name])

    offsets = np.arange(size)
    increm = 0
    fig, ax = plt.subplots()
    for algorithm in algorithm_dict:
        ax.bar(offsets + increm, algorithm_dict[algorithm], color=plot_helper.get_color(algorithm), width = 0.25)
        increm += 0.25

    ax.set_ylabel("cost")
    ax.set_title(' Costs by individual Querries')
    ax.set_xticks(offsets, queries)

    plt.show()
    fig.show()


def overal_costs_breakdown(data: List[BenchmarkDataclass]):

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
        for i, query_cost in enumerate(item.costs_by_query):
            query = item.queries[i]
            if query not in budgets[item.budget_in_bytes].keys():
                budgets[item.budget_in_bytes][query] = {}
            budgets[item.budget_in_bytes][query].update(
                {item.sequence: query_cost["Cost"]}
            )
    return budgets


def combine_data_files(data_paths: List[str]) -> List[BenchmarkDataclass]:
    data: List[BenchmarkDataclass] = []

    for item in data_paths:
        data += extract_entries(item, "for plotting")
    return data


data = combine_data_files(
    [
        "/Users/Julius/masterarbeit/Masterarbeit-JStreit/data/baseline_measures/results_extend_tpcds_90_queries.csv",
        "/Users/Julius/masterarbeit/Masterarbeit-JStreit/data/baseline_measures/results_relaxation_tpcds_90_queries.csv",
    ]
)


plot_helper = PlotHelper()


with open("costs.json", "w+") as file:
    json.dump(overal_costs_breakdown(data),file, indent=4)
    pass

with open("indexes.json", "w+") as file:
    json.dump(equal_index_configs_by_budget(data),file, indent=4)
    pass

with open("indexes_by_budget.json", "w+") as file:
    json.dump(indexes_by_budget(data),file, indent=4)
    pass

with open("costbyquery.json", "w+") as file:
    json.dump(costs_by_query(data), file, indent=4)
    pass

plot_costs_by_query_combined(costs_by_query(data), 20000000000, plot_helper)

# TPCH =46164891.51 TPCDS=121150974.81
plot_overall_costs(data, [], 46164891, plot_helper)

plot_runtime(data, plot_helper)

plot_costs_by_query_individual(costs_by_query(data), [20000000000], plot_helper)
