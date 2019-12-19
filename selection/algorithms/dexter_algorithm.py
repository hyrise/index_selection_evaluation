from ..selection_algorithm import SelectionAlgorithm
import subprocess
import logging
import os


# TODO add parameter for single_column / multi_column / winning
# and adjust dexter algorithm
DEFAULT_PARAMETERS = {'min_saving_percentage': 50}


class DexterAlgorithm(SelectionAlgorithm):
    def __init__(self, database_connector, parameters):
        SelectionAlgorithm.__init__(self, database_connector, parameters,
                                    DEFAULT_PARAMETERS)

    def calculate_best_indexes(self, workload):
        min_percentage = self.parameters['min_saving_percentage']
        if 'budget' in self.parameters:
            min_percentage = 0
        database_name = self.database_connector.db_name

        potential_indexes = self.potential_indexes(workload)
        indexes_found = []

        for query in workload.queries:
            command = (f'ruby dexter/lib/dexter.rb {database_name}'
                       f' --min-cost-savings-pct {min_percentage} -s " ')
            command += query.text
            command += '"'
            # TODO prepare statement if create and drop view in statement
            # and update query text
            p = subprocess.Popen(command, cwd=os.getcwd(),
                                 stdout=subprocess.PIPE,
                                 stderr=subprocess.STDOUT, shell=True)
            with p.stdout:
                output_string = p.stdout.read().decode('utf-8')
            p.wait()

            log_output = output_string.replace('\n', '')
            # log_output = output_string
            logging.debug(f'{query}: {log_output}')

            if 'public.' in output_string:
                index = output_string.split('public.')[1].split(' (')
                table = index[0]
                column = index[1].split(')')[0]
                indexes_found.append(f'{table};{column}')
            # TODO support multicolumn indices
        indexes = []
        for index in potential_indexes:
            column = index.columns[0]
            if f'{column.table.name};{column.name}' in indexes_found:
                indexes.append(index)

        if 'budget' in self.parameters:
            # find indexes in budget
            new = []
            # what_if = self.cost_evaluation.what_if
            budget = self.parameters['budget'] * 1000000
            current_size = 0

            while True:
                initial = self.cost_evaluation.calculate_cost(workload,
                                                              new)
                best_ratio = [None, 0, 0]
                for i in indexes:
                    cost = self.cost_evaluation.calculate_cost(workload,
                                                               new + [i],
                                                               store_size=True)
                    benefit = initial - cost
                    size = i.estimated_size
                    ratio = benefit / size
                    # print(i)
                    # print(cost)
                    # print(ratio)
                    if best_ratio[1] < ratio and current_size + size < budget:
                        best_ratio = [i, ratio, cost]

                        # print('replace')
                        # print(i)
                if best_ratio[1] == 0:
                    return new
                else:
                    print('\nadd index')
                    print(best_ratio)
                    indexes.remove(best_ratio[0])
                    current_size += best_ratio[0].estimated_size
                    new.append(best_ratio[0])
            return new
        return indexes
