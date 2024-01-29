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
            indexes = parse_index_string_list(row[-1])
            parameters = json.loads(row[3])
            run_time = json.loads(row[7])
            results.append((memory, algorithm_runtime, query_costs, indexes, parameters, run_time))
    return results


def parse_index_string_list(index_string_list):
    indexes = []
    if index_string_list == '[]':
        return []
    index_string_list = index_string_list.strip('[]')
    index_string_list = index_string_list.split(', ')
    for index_string in index_string_list:
        index_string = index_string[2:-1]
        index = []
        for index_attribute in index_string.split(','):
            index.append(index_attribute[2:])
        indexes.append(index)
    return indexes


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
                print(algorithm, (line[0], line[1], sum(line[2]), line[3], line[4]))
    else:
        result = parse_file(sys.argv[1])
        for line in result:
            print(line[0], line[1], sum(line[2]), line[3])
