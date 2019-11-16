# Index selection

Install Python and PostgreSQL.
* `brew install python3`
* `brew install postgres`
* `pip3 install psycopg2-binary`

Create PostgreSQL user with superuser privileges.
```
sudo -u postgres createuser -s $(whoami);
eval "sudo -u postgres psql -c 'alter user \"$(whoami)\" with superuser;'"
```

Install development tools for TPC-H or TPC-DS generation:
* https://github.com/gregrahn/tpch-kit
* https://github.com/gregrahn/tpcds-kit

Install Ruby (with RVM).

Update submodules
* `git submodule update --init --recursive`
* `cd dexter`
* `gem install rake`

Start postgres service.
* `brew services start postgresql`

Install HypoPG (PostgreSQL extension to simulate indexes):
* Download `tar.gz` (https://github.com/HypoPG/hypopg/releases)
* Decompress
* `sudo make install`

Run python scripts:
* `python3 -m selection`
(Using the parameters from `config.json`)

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
