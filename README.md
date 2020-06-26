# Index Selection Evaluation

This repository contains the source code for the evaluation platform presented in the paper *An Experimental Evaluation of Index Selection Algorithms*. As part of this paper, we re-implemented 8 index selection algorithms ([references](#references) listed below):

- The drop heuristic [1]
- An algorithm similar to the initial AutoAdmin algorithm [2]
- An algorithm loosely following the DB2 advisor index selection [3]
- The Relaxation algorithm [4]
- CoPhy's approach [5]
- Dexter [6]
- The Extend algorithm [7]
- An algorithm loosely following SQLServer's DTA Anytime index selection [8]

The implementations of the algorithms can be found under `selection/algorithms`. Documentation, also regarding the parameters of the algorithms, is part of the source files.

While some of the chosen algorithms are related to tools employed in commercial DBMS products, the re-implemented algorithms do not fully reflect the behavior and performance of the original tools, which may be continuously enhanced and optimized.

## Usage

Install script:
* `./scripts/install.sh`

Run index selection evaluation for the TPC-H benchmark:
* `python3 -m selection benchmark_results/tpch_wo_2_17_20/config.json`
(If the last parameter is omitted, the default config `example_configs/config.json` is used)

Run tests:
* `python3 -m unittest discover tests`

Get coverage:
```
coverage run --source=selection -m unittest discover tests/
coverage html
open htmlcov/index.html
```

## Adding a new algorithm:
* Create a new algorithm class, based on `selection/algorithms/example_algorithm.py`
* Add algorithm class name in `selection/index_selection_evaluation.py` to this dictionary:
```
ALGORITHMS = {'microsoft': MicrosoftAlgorithm,
              'drop_heuristic': DropHeuristicAlgorithm}
```
* Create or adjust configuration files


## Formatting and Linting
The code can be automatically formatted/linted by calling `./scripts/format.sh` or `./scripts/lint.sh` from the main folder.

# References
[1] (Drop): Kyu-Young Whang: Index Selection in Relational Databases. FODO 1985: 487-500

[2] (AutoAdmin): Surajit Chaudhuri, Vivek R. Narasayya: An Efficient Cost-Driven Index Selection Tool for Microsoft SQL Server. VLDB 1997: 146-155

[3] (DB2Advis): Gary Valentin, Michael Zuliani, Daniel C. Zilio, Guy M. Lohman, Alan Skelley: DB2 Advisor: An Optimizer Smart Enough to Recommend Its Own Indexes. ICDE 2000: 101-110

[4] (Relaxation): Nicolas Bruno, Surajit Chaudhuri: Automatic Physical Database Tuning: A Relaxation-based Approach. SIGMOD Conference 2005: 227-238

[5] (CoPhy): Debabrata Dash, Neoklis Polyzotis, Anastasia Ailamaki: CoPhy: A Scalable, Portable, and Interactive Index Advisor for Large Workloads. Proc. VLDB Endow. 4(6): 362-372 (2011)

[6] (Dexter): Andrew Kane:  https://medium.com/@ankane/introducing-dexter-the-automatic-indexer-for-postgres-5f8fa8b28f27

[7] (Extend): Rainer Schlosser, Jan Kossmann, Martin Boissier: Efficient Scalable Multi-attribute Index Selection Using Recursive Strategies. ICDE 2019: 1238-1249

[8] (Anytime): Not published yet. DTA documentation: https://docs.microsoft.com/de-de/sql/tools/dta/dta-utility?view=sql-server-ver15
