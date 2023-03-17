from string import Template

template = Template("""
option solver '$solver';
$relaxation_str

set BUDGETS = $budget_str;

model ilp_model.mod;

data $input_file;
$additional_input;

check {q in QUERIES}: costs[q, 0] != 99999999999999;
param overall_cost := sum {q in QUERIES} costs[q, 0];

display QUERIES, NUMBER_OF_INDEXES, NUMBER_OF_INDEX_COMBINATIONS, overall_cost;
display QUERIES, NUMBER_OF_INDEXES, NUMBER_OF_INDEX_COMBINATIONS, overall_cost > ("$output_file");
printf "\\n" >> ("$output_file");

for {c_budget in BUDGETS} {
	let storage_budget := c_budget * 1000 * 1000;
	solve;

	display storage_budget, overall_costs_of_queries/overall_cost, _solve_elapsed_time;
	display storage_budget, overall_costs_of_queries/overall_cost, _solve_elapsed_time >> ("$output_file");

	printf "___Used index combinations per query:\\n";
	printf "___Used index combinations per query:\\n" >> ("$output_file");

	print {q in QUERIES, k in COMBINATIONS: z[k,q] = 1} k;
	print {q in QUERIES, k in COMBINATIONS: z[k,q] = 1} k >> ("$output_file");
	printf "\\n\\n" >> ("$output_file");
}
""")
