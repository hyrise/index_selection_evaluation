from string import Template

template = Template("""
option solver '$solver';
#option gurobi_options 'mipgap=0.01';

model ilp_model.mod;

data $ampl_data_file;

check {q in QUERIES}: costs[q, 0] != 99999999999999;
param overall_cost := sum {q in QUERIES} costs[q, 0];

let storage_budget := $storage_budget * 1000 * 1000;

param number_of_combinations = $number_of_combinations;
param number_of_chunks = $number_of_chunks;

for {i in 0..number_of_chunks-1}{
	let SELECTED_COMBINATIONS := $fixed_combinations union i .. number_of_combinations by number_of_chunks;
	display i, card(SELECTED_COMBINATIONS);
	solve;
	printf "___%d\t%f\t%f\\n", storage_budget, overall_costs_of_queries/overall_cost, _solve_elapsed_time;
	printf "___%d\t%f\t%f\\n", storage_budget, overall_costs_of_queries/overall_cost, _solve_elapsed_time  > ("$file_name");
	display i, card(SELECTED_COMBINATIONS) >> ("$file_name");
	printf "combinations:" >> ("$file_name");
	print {q in QUERIES, k in SELECTED_COMBINATIONS: z[k,q] = 1} k;
	print {q in QUERIES, k in SELECTED_COMBINATIONS: z[k,q] = 1} k  >> ("$file_name");
}
""")

template2 = Template("""
option solver '$solver';
#option gurobi_options 'mipgap=0.01';

model ilp_model.mod;

data $ampl_data_file;

check {q in QUERIES}: costs[q, 0] != 99999999999999;
param overall_cost := sum {q in QUERIES} costs[q, 0];
param overall_time := $overall_time;

let storage_budget := $storage_budget * 1000 * 1000;

let SELECTED_COMBINATIONS := {$combination_string};
display card(SELECTED_COMBINATIONS);
solve;
printf "___%d\t%f\t%f\\n", storage_budget, overall_costs_of_queries/overall_cost, _solve_elapsed_time + overall_time;
printf "___%d\t%f\t%f\\n", storage_budget, overall_costs_of_queries/overall_cost, _solve_elapsed_time + overall_time  > ("$file_name2");
#display _solve_elapsed_time;
printf "combinations:" >> ("$file_name2");
print {q in QUERIES, k in SELECTED_COMBINATIONS: z[k,q] = 1} k;
print {q in QUERIES, k in SELECTED_COMBINATIONS: z[k,q] = 1} k  >> ("$file_name2");
""")
