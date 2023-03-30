import json

from selection.algorithms.cophy_input_generation import output_as_json, output_as_ampl


input_file1 = 'ILP/tpch_relaxation_1_cache_input.json'
input_file2 = 'ILP/tpch_relaxation_2_cache_input.json'
#input_file2 = 'ILP/tpch_relaxation_3_cache_input.json'

input_file1 = 'ILP/tpch_combined_relaxation12_input.json'
input_file2 = 'ILP/tpch_relaxation_3_cache_input.json'


input_file1 = 'ILP/tpch_combined_extend123_input.json'
input_file2 = 'ILP/tpch_combined_relaxation123_input.json'

input_file1 = 'ILP/tpcds_extend_1_cache_input.json'
input_file2 = 'ILP/tpcds_extend_2_cache_input.json'


index_sizes = {}        # used to store actual information
query_costs_for_index_combination = {}     # used to store actual information
queries = None

with open(input_file1) as f:
    data = json.loads(f.read())
    queries = data['queries']

    index_per_id = {}   # used to speed up the lookup
    for index in data['index_sizes']:
        index_id, estimated_size, column_names = index['index_id'], index['estimated_size'], index['column_names']
        index_sizes[tuple(column_names)] = estimated_size
        index_per_id[index_id] = tuple(column_names)
    print(index_sizes)
    print(index_per_id)

    combination_per_id = {}     # used to speed up the lookup
    for index_combination in data['index_combinations']:
        combination_id, index_ids = index_combination['combination_id'], index_combination['index_ids']
        print(index_ids)
        index_set = frozenset([index_per_id[index_id] for index_id in index_ids])
        query_costs_for_index_combination[index_set] = {}
        combination_per_id[combination_id] = index_set

    for q_costs in data['query_costs']:
        query_number, combination_id, costs = q_costs['query_number'], q_costs['combination_id'], q_costs['costs']
        index_set = combination_per_id[combination_id]
        query_costs_for_index_combination[index_set][query_number] = costs


with open(input_file2) as f:
    data = json.loads(f.read())
    assert data['queries'] == queries

    index_per_id = {}  # reset lookup structure;  used to speed up the lookup
    for index in data['index_sizes']:
        index_id, estimated_size, column_names = index['index_id'], index['estimated_size'], index['column_names']
        column_names = tuple(column_names)
        if column_names not in index_sizes:
            index_sizes[column_names] = estimated_size
        else:
            assert index_sizes[column_names] == estimated_size
        index_per_id[index_id] = tuple(column_names)

    combination_per_id = {}  # reset lookup structure;  used to speed up the lookup
    for index_combination in data['index_combinations']:
        combination_id, index_ids = index_combination['combination_id'], index_combination['index_ids']
        index_set = frozenset([index_per_id[index_id] for index_id in index_ids])
        if index_set not in query_costs_for_index_combination:
            query_costs_for_index_combination[index_set] = {}
        combination_per_id[combination_id] = index_set

    for q_costs in data['query_costs']:
        query_number, combination_id, costs = q_costs['query_number'], q_costs['combination_id'], q_costs['costs']
        index_set = combination_per_id[combination_id]
        if query_number not in query_costs_for_index_combination[index_set]:
            query_costs_for_index_combination[index_set][query_number] = costs
        else:
            assert query_costs_for_index_combination[index_set][query_number] == costs

ilp_dict = {
            "what_if_time": None,
            "cost_requests": None,
            "cache_hits": None,
            "number_of_indexes": len(index_sizes),
            "number_of_index_combinations": len(query_costs_for_index_combination) - 1, # do not count empty configuration
            "queries": queries,
            "index_sizes": [],
            "index_combinations": [],
            "query_costs": [],
        }

# store index sizes and determine index_ids for later use in combinations
index_ids = {}
for i, column_names in enumerate(sorted(index_sizes)):
    ilp_dict["index_sizes"].append(
        {
            "index_id": i + 1,
            "estimated_size": index_sizes[column_names],
            "column_names": column_names,
        }
    )
    index_ids[column_names] = i + 1

for i, index_combination in enumerate(query_costs_for_index_combination):
    index_id_list = [index_ids[index] for index in index_combination]
    ilp_dict["index_combinations"].append(
        {"combination_id": i, "index_ids": index_id_list}
    )

# store query costs per query and index_combination
for query_number in queries:
    for i, index_combination in enumerate(query_costs_for_index_combination):
        # query is in dictionary if cost is lower than default or present in cache
        if query_number in query_costs_for_index_combination[index_combination]:
            ilp_dict["query_costs"].append(
                {
                    "query_number": query_number,
                    "combination_id": i,
                    "costs": query_costs_for_index_combination[index_combination][
                        query_number
                    ],
                }
            )

# output_as_json(ilp_dict, 'ILP/tpch_combined_extend12_input.json')
# output_as_ampl(ilp_dict, 'ILP/tpch_combined_extend12_input.txt')

# output_as_json(ilp_dict, 'ILP/tpch_combined_extend123_input.json')
# output_as_ampl(ilp_dict, 'ILP/tpch_combined_extend123_input.txt')


# output_as_json(ilp_dict, 'ILP/tpch_combined_relaxation12_input.json')
# output_as_ampl(ilp_dict, 'ILP/tpch_combined_relaxation12_input.txt')

# output_as_json(ilp_dict, 'ILP/tpch_combined_relaxation123_input.json')
# output_as_ampl(ilp_dict, 'ILP/tpch_combined_relaxation123_input.txt')


output_as_json(ilp_dict, 'ILP/tpcds_combined_extend12_input.json')
output_as_ampl(ilp_dict, 'ILP/tpcds_combined_extend12_input.txt')








