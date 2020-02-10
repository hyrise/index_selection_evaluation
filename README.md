# Index Selection Evaluation

Install script:
* `./scripts/install.sh`

Run python scripts:
* `python3 -m selection`
(Using the parameters in `example_configs/config.json`)

Run tests:
* `python3 -m unittest discover tests`

Get coverage:
```
coverage run --source=selection -m unittest discover tests/
coverage html
open htmlcov/index.html
```

# Adding a new algorithm:
* Create a new algorithm class, based on `selection/algorithms/example_algorithm.py`
* Add algorithm class name in `selection/index_selection_evaluation.py` to this dictionary:
```
ALGORITHMS = {'microsoft': MicrosoftAlgorithm,
              'drop_heuristic': DropHeuristicAlgorithm}
```
* Create or adjust configuration files


# Formatting
The code can be automatically formatted by calling ./scripts/format.sh from the main folder.