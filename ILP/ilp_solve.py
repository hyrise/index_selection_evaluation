from ilp_solve_template import template
from settings import ampl_path, solver_path

import subprocess

def main(benchmark, algorithm):
    ampl = ampl_path
    solver = solver_path

    input_file = f'{benchmark}_{algorithm}_input.txt'
    cmd_file = f'{benchmark}_{algorithm}_solve.cmd'
    output_file = f'{benchmark}_{algorithm}_solution.txt'

    additional_input = ''
    if 'relaxation' in algorithm:
        additional_input = f'''update data costs;
        data {folder}{benchmark}_no_index_cache_input.txt;'''

    budget_str = f'{{{", ".join(map(str, range(500, 15001, 500)))}}}'

    with open(cmd_file, 'w') as f:
        f.write(template.substitute(
            solver=solver,
            input_file=input_file,
            additional_input=additional_input,
            output_file=output_file,
            budget_str=budget_str,
            relaxation_str = ''
        ))
    p = subprocess.Popen([ampl, cmd_file])
    p.wait()


if __name__ == "__main__":
    pass
    # Examples:
    # main('tpch', 'cophy__width2__per_query2__query-based')
    # main('tpcds', "extend_1_cache")
    # main('tpch', "extend_2_cache")
    # for width, per_query in [(1,2), (2,1), (2,2)]:
    #     for enumeration in ['full', 'query-based']:
    #         main('tpch', f"cophy__width{width}__per_query{per_query}__{enumeration}")
