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
        return self.cursor.fetchall()

    def get_statistics(self):
        print('show statistics')
        try:
            result = self.run('select count(*) as c, data_statistics_type, data_source_schema_name, data_source_object_name from M_DATA_STATISTICS group by data_statistics_type, data_source_schema_name, data_source_object_name')
            print(result)
        except Exception:
            print('no statistics found')

    def create_statistics_data_schema(self):
        print('create statistics')
        self.run('create statistics on indexselection_tpch___1.lineitem type histogram REFRESH TYPE manual enable on')
        print('create statistics done')

    def export_statistics(self):
        print('export')
        self.run("export indexselection_tpch___1.lineitem as csv into '/usr/sap/JA1/HDB00/work/newexport' with replace all statistics only")
        print('import')
        self.run("import indexselection_tpch___1.lineitem as csv from '/usr/sap/JA1/HDB00/work/newexport' with rename schema indexselection_tpch___1 to tpchsf1_test_import replace statistics only")

    def get_cost(self):
        self.connection.commit()
        result = self.cursor.execute("explain plan set statement_name = 'test1' for select l_comment from indexselection_tpch___1.lineitem where l_comment = 'abc'")
        import pdb; pdb.set_trace()


def main():
    with open('connection.json', 'r') as file:
        connection_data = json.load(file)
    db_conn = DatabaseConnector(*connection_data)
    db_conn.get_statistics()
    db_conn.drop_all_statistics()
    db_conn.get_statistics()
    db_conn.create_statistics_data_schema()
    db_conn.export_statistics()
    db_conn.get_statistics()

    db_conn.get_cost()


    #  db_conn.drop_all_statistics()
    db_conn.get_statistics()


if __name__ == '__main__':
    main()
