import json

from analysis import (
    combine_data_files,
    costs_by_query,
    equal_index_configs_by_budget,
    indexes_by_budget,
    overall_costs_breakdown,
    compare_algorithm_costs,
)
from make_plots import plot_costs_by_query_individual, plot_overall_costs, plot_runtime
from plot_helper import PlotHelper

OVERALL_COSTS_BREAKDOWN = True
INDEX_CONFIGS_BY_BUDGET = True
INDEXES_BY_BUDGET = True
COSTS_BY_QUERY = True
PLOT_OVERALL_COSTS = True
PLOT_RUNTIMES = True
PLOT_COSTS_BY_QUERY = True
COMPARE_ALGORITHMS = True


plot_helper = PlotHelper()
data = combine_data_files(
    [
        "/Users/Julius/masterarbeit/Masterarbeit-JStreit/data/baseline_measures/results_extend_tpcds_90_queries.csv",
        "/Users/Julius/masterarbeit/Masterarbeit-JStreit/data/baseline_measures/results_relaxation_tpcds_90_queries.csv",
        "/Users/Julius/masterarbeit/Masterarbeit-JStreit/data/baseline_measures/results_db2advis_tpcds_90_queries.csv",
    ],
    "/Users/Julius/masterarbeit/Masterarbeit-JStreit/data/baseline_plans/plans",
)

if OVERALL_COSTS_BREAKDOWN:
    with open("costs.json", "w+") as file:
        json.dump(overall_costs_breakdown(data), file, indent=4)
        pass

if INDEX_CONFIGS_BY_BUDGET:
    with open("equal_indexes.json", "w+") as file:
        json.dump(equal_index_configs_by_budget(data), file, indent=4)
        pass

if INDEXES_BY_BUDGET:
    with open("indexes_by_budget.json", "w+") as file:
        json.dump(indexes_by_budget(data), file, indent=4)
        pass

if COSTS_BY_QUERY:
    with open("costbyquery.json", "w+") as file:
        json.dump(costs_by_query(data), file, indent=4)
        pass

if COMPARE_ALGORITHMS:
    with open("algorithm_comparison.json", "w+") as file:
        json.dump(
            compare_algorithm_costs(
                data, 20000000000, "db2advis-2", ["db2advis-3"]
            ),
            file,
            indent=4,
        )

if PLOT_OVERALL_COSTS:
    # TPCH =46164891.51 TPCDS=121150974.81
    plot_overall_costs(data, [], 121150974, plot_helper)

if PLOT_RUNTIMES:
    plot_runtime(data, plot_helper)

if PLOT_COSTS_BY_QUERY:
    plot_costs_by_query_individual(costs_by_query(data), [20000000000], plot_helper)
