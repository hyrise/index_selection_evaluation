from pathlib import Path
import subprocess
import time
from typing import List


def model_gen(model_path: str, budget: int):
    """Generates an AMPL model"""
    model_string = '''
set QUERIES;
param NUMBER_OF_INDEXES integer;
param NUMBER_OF_INDEX_COMBINATIONS integer;

set INDEXES = 1 .. NUMBER_OF_INDEXES;
# 0 represents no index
set COMBINATIONS = 0 .. NUMBER_OF_INDEX_COMBINATIONS;
'''
    model_string += f'param storage_budget:= {budget};'
    model_string +='''
    set indexes_per_combination {COMBINATIONS};

param size {INDEXES}; # size of index
param costs {QUERIES, COMBINATIONS} default 99999999999999; # costs of combination for query

var x {INDEXES} binary; # index is created
var y {COMBINATIONS} binary; # combination is applicable
var z {COMBINATIONS, QUERIES} binary; # combination is used for query


minimize overall_costs_of_queries: sum {c in COMBINATIONS, q in QUERIES} costs[q, c] * z[c, q];

subject to one_combination_per_query {q in QUERIES}: 1 = sum {c in COMBINATIONS} z[c, q];
subject to applicable_combination {c in COMBINATIONS}: sum {i in indexes_per_combination[c]} x[i] >= card(indexes_per_combination[c]) * y[c];
subject to usable_combination {c in COMBINATIONS, q in QUERIES}: z[c, q] <= y[c];
subject to memory_consumption: sum {i in INDEXES} x[i] * size[i] <= storage_budget;
'''
    with open(model_path, 'w+', encoding='utf-8') as file:
        file.write(model_string)
    return model_string

def generate_run_file(run_path: str, solver_path: str):
    """Generates the run File"""
    run_string = f'''option solver '{solver_path}';
solve;
option display_1col 10000000000000000000000000;
display x;
display y;
display z;'''
    with open(run_path, 'w+', encoding='utf-8') as file:
        file.write(run_string)
    return run_string

def solve(  input_file_path: str,
            ampl_path: str,
            run_file: str,
            model_path: str,
            budget: int,
        ):
    """Solves a given data file"""
    path = Path(input_file_path)
    model_gen(model_path, budget)
    with open(f'solves/{path.name}-{budget}-out.solve', 'w+', encoding='utf-8') as outfile:
        start = time.time()
        subprocess.run([ampl_path, model_path, path, run_file], check=False ,stdout=outfile)
        outfile.write(f'\nTime: {time.time() - start}')

    print(f'completed {path.name} {budget}')

def mb_to_b(budgets: List[int]) -> List[int]:
    """Converts MB to B"""
    new_budget = []
    for cost in budgets:
        new_budget.append(cost * 1000 * 1000)
    return new_budget

# put cophy input files here
DATA_FILES = []

# This should really be a config :|
MODEL_PATH = "temp.mod"
RUN_FILE_PATH = "/Users/Julius/masterarbeit/J-Index-Selection/cophy_tools/run.run"
AMPL_PATH = '/Users/Julius/masterarbeit/ampl_macos64/ampl'
SOLVER_PATH = '/Users/Julius/masterarbeit/ampl_macos64/gurobi'


budgets = [500, 1000, 1500, 2000, 2500, 3000, 3500, 4000, 4500, 5000, 5500, 6000, 6500, 7000, 7500, 8000, 8500, 9000, 9500, 10000, 10500, 11000, 11500, 12000, 12500, 13000, 13500, 14000, 14500, 15000, 15500, 16000, 16500, 17000, 17500, 18000, 18500, 19000, 19500, 20000]
budgets = mb_to_b(budgets)

generate_run_file(RUN_FILE_PATH, SOLVER_PATH)
for item in DATA_FILES:
    for input_budget in budgets:
        solve(item, AMPL_PATH, RUN_FILE_PATH, MODEL_PATH, input_budget)
