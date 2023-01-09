import os
from pathlib import Path
import subprocess
from typing import List
import time


def model_gen(indexes: int, combinations: int, budget: int, query_string: str, model_path: str):
    modstring = f'''param budget := {budget};
set INDEXES = 1..{indexes};
set COMBINATIONS = 0..{combinations};\n
set QUERIES = '''
    modstring += '{'
    modstring += f'{query_string}'
    modstring += '''};
set combi {COMBINATIONS};
param a {INDEXES}; # size of index
param f4 {QUERIES, COMBINATIONS} default 99999999999; # costs of combination for query
var x {INDEXES} binary; # index is created
var y {COMBINATIONS} binary; # combination is applicable
var z {COMBINATIONS, QUERIES} binary; # combination is used for query
minimize overall_costs_of_queries: sum {c in COMBINATIONS, q in QUERIES} f4[q, c] * z[c, q];
subject to one_combination_per_query {q in QUERIES}: 1 = sum {c in COMBINATIONS} z[c, q];
subject to applicable_combination {c in COMBINATIONS}: sum {i in combi[c]} x[i] >= card(combi[c]) * y[c];
subject to usable_combination {c in COMBINATIONS, q in QUERIES}: z[c, q] <= y[c];
subject to memory_consumption: sum {i in INDEXES} x[i] * a[i] <= budget;'''
    with open(model_path, 'w+') as file:
        file.write(modstring)
    return modstring

def generate_run_file(run_path: str, solver_path: str):
    run_string = f'''option solver '{solver_path}';
solve;
display x;
display y;
display z;'''
    with open(run_path, 'w+') as file:
        file.write(run_string)
    return run_string


def generate_query_string(number: int, out: List[int]) -> str:
    query_list = []
    for i in range(number):
        if i+1 not in out:
            query_list.append(f'{i+1}')
    return ",".join(query_list)

def solve(  in_path: str,
            ampl_path: str,
            run_file: str,
            model_path: str,
            budget: int,
            query_string: str):
    path = Path(in_path)
    with open(path, 'r')as file:
        line = file.readline()
        indexes = int(line[1:])
        line = file.readline()
        combis = int(line[1:])

    model_gen(indexes, combis, budget, query_string, model_path)
    with open(f'{path.name}-{budget}-out.solve', 'w+') as outfile:
        start = time.time()
        subprocess.run([ampl_path, model_path, path, run_file],stdout=outfile)
        outfile.write(f'\nTime: {time.time() - start}')

    print(f'completed {path.name} {budget}')

datafiles = [   '/home/Julius.Streit/index_selection_evaluation/baseline_measurements_tpch/data/tpch_1_1-None.dat',
                '/home/Julius.Streit/index_selection_evaluation/baseline_measurements_tpch/data/tpch_2_1-None.dat',
                '/home/Julius.Streit/index_selection_evaluation/baseline_measurements_tpch/data/tpch_1_2-None.dat',
                '/home/Julius.Streit/index_selection_evaluation/baseline_measurements_tpch/data/tpch_2_2-None.dat',
                '/home/Julius.Streit/index_selection_evaluation/baseline_measurements_tpch/data/tpch_3_1-None.dat',
                '/home/Julius.Streit/index_selection_evaluation/baseline_measurements_tpch/data/tpch_1_3-None.dat'
            ]

model_path = "temp.mod"
runfile = "/home/Julius.Streit/index_selection_evaluation/run.run"
amplpath = '/home/Julius.Streit/ampl_linux-intel64/ampl'
solverpath = '/home/Julius.Streit/ampl_linux-intel64/gurobi'


budgets = [1000000000, 2000000000, 3000000000, 4000000000, 5000000000, 6000000000, 7000000000, 8000000000, 9000000000, 10000000000, 11000000000, 12000000000, 13000000000, 14000000000, 15000000000]

query_string = generate_query_string(22, [2, 17, 20])

generate_run_file(runfile, solverpath)
for item in datafiles:
    for budget in budgets:
        solve(item, amplpath, runfile, model_path, budget, query_string)
