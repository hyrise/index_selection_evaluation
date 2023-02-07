from typing import Dict, List

import matplotlib.pyplot as plt
import numpy as np
from benchmark_dataclass import BenchmarkDataclass
from plot_helper import PlotHelper


def plot_runtime(data: List[BenchmarkDataclass], plot_helper: PlotHelper):
    """Plots the runtime of algorithms."""
    config_run_times = {}
    config_budgets = {}

    for item in data:
        if item.sequence not in config_run_times.keys():
            config_run_times[item.sequence] = []
            config_budgets[item.sequence] = []

        config_run_times[item.sequence].append(float(item.time_run_total))
        config_budgets[item.sequence].append(item.budget_in_bytes / 1000**2)

    for algorithm, _ in config_budgets.items():
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
    plt.title(f"Algorithm Runtime on {data[0].benchmark}")
    plt.legend()
    plt.show()


def plot_overall_costs(
    data: List[BenchmarkDataclass],
    removes: List[str],
    no_index_cost: float,
    plot_helper: PlotHelper,
):
    """Plots the overall costs of algorithms"""

    config_overall_costs: Dict[str, List[float]] = {}
    config_budgets: Dict[str, List[float]] = {}

    for data_object in data:
        if data_object.sequence in removes:
            continue
        if data_object.sequence not in config_overall_costs:
            config_overall_costs[data_object.sequence] = []
            config_budgets[data_object.sequence] = []

        config_overall_costs[data_object.sequence].append(
            data_object.overall_costs / no_index_cost
        )
        config_budgets[data_object.sequence].append(
            data_object.budget_in_bytes / 1000**2
        )

    for algorithm, value in config_budgets.items():
        plt.step(
            value,
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

            # This makes sure that the costs are not all the same, and therefor worthless.
            costs_set = set(costs)
            if len(costs_set) == 1:
                continue

            plt.bar(algorithms, costs, color=colors)
            plt.xlabel("Algorithm")
            plt.ylabel("Cost")
            plt.title(f"Costs for {query}")
            plt.show()


def plot_costs_by_query_combined(
    dictionary: Dict[str, Dict[str, Dict[str, str]]], budget: int, plot_helper: PlotHelper
):
    # THIS CODE DOES NOT MEANINGFULLY WORK AS IS
    # It does the thing but it does not do it well
    data = dictionary[budget]
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
    increment = 0
    fig, ax = plt.subplots()
    for algorithm, value in algorithm_dict.items():
        ax.bar(
            offsets + increment,
            value,
            color=plot_helper.get_color(algorithm),
            width=0.25,
        )
        increment += 0.25

    ax.set_ylabel("cost")
    ax.set_title(" Costs by individual Querries")
    ax.set_xticks(offsets, queries)

    plt.show()
    fig.show()

def plot_cost_comparison():

    pass