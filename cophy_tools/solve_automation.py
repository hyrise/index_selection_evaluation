import os
from pathlib import Path
import subprocess


def model_gen(indexes: int, combinations: int, budget: int):
    modstring = f'''param budget := {budget};
set INDEXES = 1..{indexes};
set COMBINATIONS = 0..{combinations};\n'''
    modstring += '''
set QUERIES;
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
    with open('temp.mod', 'w+') as file:
        file.write(modstring)
    return modstring

datafiles = "/Users/Julius/masterarbeit/J-Index-Selection/testificate/data/"
runfile = "/Users/Julius/masterarbeit/J-Index-Selection/cophy_tools/run.run"
amplpath = '/Users/Julius/masterarbeit/ampl_macos64/ampl'
model_path = "temp.mod"
budget = 1000000000

for item in os.listdir(datafiles):
    path = f'{datafiles}{item}'
    with open(path, 'r')as file:
        line = file.readline()
        indexes = int(line[1:])
        line = file.readline()
        combis = int(line[1:])

    model_gen(indexes, combis, budget)
    with open(f'{item}-out.txt', 'w+') as outfile:
        subprocess.run([amplpath, model_path, path, runfile],stdout=outfile)