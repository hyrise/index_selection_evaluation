import csv
import json
import sys


def parse_file(file_name):
    results = []
    with open(file_name, newline="") as csv_file:
        result_reader = csv.reader(csv_file, delimiter=";")
        for i, row in enumerate(result_reader):
            if i == 0:
                continue
            parameters = json.loads(row[3])
            if "budget_MB" in parameters:
                memory = parameters["budget_MB"]
            else:
                memory = float(row[13]) / 10**6
            algorithm_runtime = float(row[7])
            query_costs = []
            for query_cost_str in row[16:-1]:
                query_cost = json.loads(query_cost_str)["Cost"]
                query_costs.append(query_cost)
            results.append((memory, algorithm_runtime, query_costs))
    return results


if __name__ == "__main__":
    if len(sys.argv) == 1:
        for algorithm in [
            "anytime",
            "auto_admin",
            "db2advis",
            "dexter",
            "drop",
            "extend",
            "no_index",
            "relaxation",
        ]:
            result = parse_file(
                f"../benchmark_results/results_{algorithm}_tpch_19_queries.csv"
            )
            for line in result:
                print(algorithm, (line[0], line[1], sum(line[2])))
    else:
        result = parse_file(sys.argv[1])
        for line in result:
            print(line[0], line[1], sum(line[2]))
