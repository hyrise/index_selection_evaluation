from typing import List
from read_csv import extract_entries
from benchmark_dataclass import BenchmarkDataclass
import matplotlib.pyplot as plt
import json

def make_runtime_plots(data: List[BenchmarkDataclass]):


    config_run_times = {}
    config_budgets = {}

    for item in data:
        if item.sequence not in config_run_times.keys():
            config_run_times[item.sequence] = []
            config_budgets[item.sequence] = []

        config_run_times[item.sequence].append(item.time_run_total)
        config_budgets[item.sequence].append(item.budget_in_bytes/1000**2)

    for item in config_budgets.keys():
        plt.step(config_budgets[item], config_run_times[item], where='mid', label = item)

    plt.xlabel('Budget')
    plt.ylabel('Runtime Algorithm')
    plt.title(f'Algortihm Runtime on {data[0].benchmark}')
    plt.legend()
    plt.show()

def make_overall_costs_plots(data: List[BenchmarkDataclass], removes: List[str], no_index_cost: float):

    config_overall_costs = {}
    config_budgets = {}

    for item in data:
        if item.sequence in removes:
            continue
        if item.sequence not in config_overall_costs.keys():
            config_overall_costs[item.sequence] = []
            config_budgets[item.sequence] = []

        config_overall_costs[item.sequence].append(item.overall_costs)
        config_budgets[item.sequence].append(item.budget_in_bytes/1000**2)

    for item in config_budgets.keys():
        plt.step(config_budgets[item], config_overall_costs[item], where='mid', label = item)

    plt.xlabel('Budget')
    plt.ylabel('Costs Algorithm')
    plt.title(f'Algortihm Total costs on {data[0].benchmark}')
    plt.legend()
    plt.show()

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

def combine_data_files(data_paths: List[str]) -> List[BenchmarkDataclass]:
    data: List[BenchmarkDataclass] = []

    for item in data_paths:
        data += extract_entries(item, 'for plotting')
    return data

data = combine_data_files(['/Users/Julius/masterarbeit/Masterarbeit-JStreit/data/baseline_measures/results_extend_tpch_19_queries.csv', '/Users/Julius/masterarbeit/Masterarbeit-JStreit/data/baseline_measures/results_relaxation_tpch_19_queries.csv'])

make_overall_costs_plots(data, [], 1)

make_runtime_plots(data)

with open('costs.json', 'w+') as file:
    json.dump(overal_costs_breakdown(data),file, indent=4)

with open('indexes.json', 'w+') as file:
    json.dump(equal_index_configs_by_budget(data),file, indent=4)
