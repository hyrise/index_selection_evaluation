from typing import List
from read_csv import extract_entries
from benchmark_dataclass import BenchmarkDataclass
import matplotlib.pyplot as plt
import json

def make_runtime_plots(data_paths: List[str]):

    data: List[BenchmarkDataclass] = []

    for item in data_paths:
        data += extract_entries(item, 'for plotting')

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

def make_overall_costs_plots(data_paths: List[str], removes: List[str], no_index_cost: float):

    data: List[BenchmarkDataclass] = []

    for item in data_paths:
        data += extract_entries(item, 'for plotting')

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

def overal_costs_breakdown(data_paths: List[str]):

    data: List[BenchmarkDataclass] = []

    for item in data_paths:
        data += extract_entries(item, 'for plotting')

    budgets = {}

    for item in data:
        if item.budget_in_bytes not in budgets.keys():
            budgets[item.budget_in_bytes] = {}
        if item.overall_costs not in budgets[item.budget_in_bytes].keys():
            budgets[item.budget_in_bytes][item.overall_costs] = []
        budgets[item.budget_in_bytes][item.overall_costs].append(item.sequence)

    return budgets

def equal_index_configs_by_budget(data_paths: List[str]):

    data: List[BenchmarkDataclass] = []

    for item in data_paths:
        data += extract_entries(item, 'for plotting')

    budgets = {}

    for item in data:
        if item.budget_in_bytes not in budgets.keys():
            budgets[item.budget_in_bytes] = {}
        sorted_indexes = str(sorted(item.selected_indexes))
        if sorted_indexes not in budgets[item.budget_in_bytes].keys():
            budgets[item.budget_in_bytes][sorted_indexes] = []
        budgets[item.budget_in_bytes][sorted_indexes].append(item.sequence)

    return budgets


make_overall_costs_plots(['/Users/Julius/masterarbeit/Masterarbeit-JStreit/data/baseline_measures/results_extend_tpcds_90_queries.csv', '/Users/Julius/masterarbeit/Masterarbeit-JStreit/data/baseline_measures/results_relaxation_tpcds_90_queries.csv'], [])

make_runtime_plots(['/Users/Julius/masterarbeit/Masterarbeit-JStreit/data/baseline_measures/results_extend_tpcds_90_queries.csv', '/Users/Julius/masterarbeit/Masterarbeit-JStreit/data/baseline_measures/results_relaxation_tpcds_90_queries.csv'])

with open('costs.json', 'w+') as file:
    json.dump(overal_costs_breakdown(['/Users/Julius/masterarbeit/Masterarbeit-JStreit/data/baseline_measures/results_extend_tpcds_90_queries.csv', '/Users/Julius/masterarbeit/Masterarbeit-JStreit/data/baseline_measures/results_relaxation_tpcds_90_queries.csv']),file, indent=4)

with open('indexes.json', 'w+') as file:
    json.dump(equal_index_configs_by_budget(['/Users/Julius/masterarbeit/Masterarbeit-JStreit/data/baseline_measures/results_extend_tpcds_90_queries.csv', '/Users/Julius/masterarbeit/Masterarbeit-JStreit/data/baseline_measures/results_relaxation_tpcds_90_queries.csv']),file, indent=4)
