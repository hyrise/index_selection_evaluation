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
        return indexes
