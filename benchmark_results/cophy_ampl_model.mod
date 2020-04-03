set QUERIES;

## TPC-H
# 3 1
# set INDEXES = 1..1215;
# set COMBINATIONS = 0..1215;

## TPC-DS
# 2 1
# set INDEXES = 1..1503;
# set COMBINATIONS = 0..1503;

## JOB
# 3 1
# set INDEXES = 1..630;
# set COMBINATIONS = 0..630;

param budget;

set combi {COMBINATIONS};

param a {INDEXES}; # size of index
param f4 {QUERIES, COMBINATIONS} default 99999999999999; # costs of combination for query

var x {INDEXES} binary; # index is created
var y {COMBINATIONS} binary; # combination is applicable
var z {COMBINATIONS, QUERIES} binary; # combination is used for query


minimize overall_costs_of_queries: sum {c in COMBINATIONS, q in QUERIES} f4[q, c] * z[c, q];

subject to one_combination_per_query {q in QUERIES}: 1 = sum {c in COMBINATIONS} z[c, q];
subject to applicable_combination {c in COMBINATIONS}: sum {i in combi[c]} x[i] >= card(combi[c]) * y[c];
subject to usable_combination {c in COMBINATIONS, q in QUERIES}: z[c, q] <= y[c];
subject to memory_consumption: sum {i in INDEXES} x[i] * a[i] <= budget;