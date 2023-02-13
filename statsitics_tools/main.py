"""The main file to handle all the analysis function"""
import json
import os

from data_handler import convert_normal_csvs, convert_cophy_csvs

from analysis import (
    compare_absolute_differences,
    compare_algorithm_costs,
    costs_by_query,
    equal_index_configs_by_budget,
    indexes_by_budget,
    overall_costs_breakdown,
)
from make_plots import (
    plot_costs_by_query_individual,
    plot_overall_costs,
    plot_runtime,
)
from plot_helper import PlotHelper


def construct_budgets(min, max, step):
    value = min
    budgets = []
    while value <= max:
        budgets.append(value)
        value += step
    return budgets


OVERALL_COSTS_BREAKDOWN = True
INDEX_CONFIGS_BY_BUDGET = True
INDEXES_BY_BUDGET = True
COSTS_BY_QUERY = True
PLOT_OVERALL_COSTS = True
PLOT_RUNTIMES = True
PLOT_COSTS_BY_QUERY = False
COMPARE_ALGORITHMS = True
SAVE_DATACLASSES = True
COMPARE_ABSOLUTE_DIFFERENCES = False

COMPARISON_BUDGET = 7000000000

plot_helper = PlotHelper()
data = convert_normal_csvs(
    [
        "/Users/Julius/masterarbeit/Masterarbeit-JStreit/data/baseline_measures_2/results_db2advis_tpch_19_queries.csv",
        "/Users/Julius/masterarbeit/Masterarbeit-JStreit/data/baseline_measures_2/results_extend_tpch_19_queries.csv",
        "/Users/Julius/masterarbeit/Masterarbeit-JStreit/data/baseline_measures_2/results_relaxation_tpch_19_queries.csv",
    ],
    "/Users/Julius/masterarbeit/Masterarbeit-JStreit/data/baseline_plans_2/plans",
)

data_cophy = convert_cophy_csvs(
    [
        "/Users/Julius/masterarbeit/Masterarbeit-JStreit/data/cophy_measures/results_cophy_input_tpch_19_queries.csv"
    ],
    construct_budgets(500000000, 20000000000, 500000000),
)

data = data + data_cophy
print(data[-1])

if OVERALL_COSTS_BREAKDOWN:
    with open("costs.json", "w+", encoding="utf-8") as file:
        json.dump(overall_costs_breakdown(data), file, indent=4)

if INDEX_CONFIGS_BY_BUDGET:
    with open("equal_indexes.json", "w+", encoding="utf-8") as file:
        json.dump(equal_index_configs_by_budget(data), file, indent=4)

if INDEXES_BY_BUDGET:
    with open("indexes_by_budget.json", "w+", encoding="utf-8") as file:
        json.dump(indexes_by_budget(data), file, indent=4)

if COSTS_BY_QUERY:
    with open("cost_by_query.json", "w+", encoding="utf-8") as file:
        json.dump(costs_by_query(data), file, indent=4)

if COMPARE_ALGORITHMS:
    out_dictionary = compare_algorithm_costs(
        data,
        COMPARISON_BUDGET,
        "relaxation-4",
        [
            "extend-1",
            "extend-2",
            "extend-3",
            "db2advis-3",
            "db2advis-7",
            "relaxation-1",
        ],
    )
    with open("algorithm_comparison.json", "w+", encoding="utf-8") as file:
        json.dump(
            out_dictionary,
            file,
            indent=4,
        )
    if COMPARE_ABSOLUTE_DIFFERENCES:
        with open("cost_sort.json", "w+", encoding="UTF-8") as file:
            file.write(
                json.dumps(compare_absolute_differences(out_dictionary), indent=4)
            )

if PLOT_OVERALL_COSTS:
    # TPCH =46164891.51 TPCDS=121150974.81
    plot_overall_costs(data, [], 46164891, plot_helper)

if PLOT_RUNTIMES:
    plot_runtime(data, plot_helper)

if PLOT_COSTS_BY_QUERY:
    plot_costs_by_query_individual(costs_by_query(data), [10000000000], plot_helper)

if SAVE_DATACLASSES:
    os.makedirs("dataclasses", exist_ok=True)
    for item in data:
        with open(
            f"dataclasses/{item.sequence}_{item.budget_in_bytes}.json",
            "w+",
            encoding="utf-8",
        ) as file:
            file.write(item.to_json(indent=4))


print("ff")
