import pyhdb
import json


class DatabaseConnector:
    def __init__(self, host, port, user, password):
        self.connection = pyhdb.connect(
            host=host,
            port=port,
            user=user,
            password=password
        )

        self.cursor = self.connection.cursor()

    def drop_all_statistics(self):
        print('drop all statistics')
        self.run('drop statistics on indexselection_tpch___1.lineitem')
        self.run('drop statistics on tpchsf1_test_import.lineitem')

    def run(self, statement):
        self.cursor.execute(statement)
        return self.cursor.fetchone()

    def get_statistics(self):
        try:
            result = self.run('select count(*) as c, data_statistics_type, data_source_schema_name, data_source_object_name from M_DATA_STATISTICS group by data_statistics_type, data_source_schema_name, data_source_object_name')
            print(result)
        except:
            print('no statistics found')


def main():
    with open('connection.json', 'r') as file:
        connection_data = json.load(file)
    db_conn = DatabaseConnector(*connection_data)
    db_conn.get_statistics()
    db_conn.drop_all_statistics()
    db_conn.get_statistics()


if __name__ == '__main__':
    main()
