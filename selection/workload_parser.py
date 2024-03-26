import glob
import os

from selection.workload import Column, Query, Table, Workload
from selection.dbms.postgres_dbms import PostgresDatabaseConnector


class WorkloadParser:
    def __init__(self, database_system, database_name, benchmark_name):
        self.database_system = database_system
        self.database_name = database_name
        self.benchmark_name = benchmark_name

    @staticmethod
    def is_custom_workload(benchmark_name):
        file_path = os.path.dirname(os.path.abspath(__file__))
        if benchmark_name in os.listdir(f"{file_path}/../custom_workloads/"):
            return True
        else:
            return False

    def get_tables(self):
        assert self.database_system == "postgres"
        db_connector = PostgresDatabaseConnector(self.database_name)
        result = db_connector.exec_fetchall(
            "SELECT tablename FROM pg_catalog.pg_tables WHERE schemaname='public';"
        )
        table_names = [row[0] for row in result]

        tables = {}

        for table_name in table_names:
            table = Table(table_name)
            result = db_connector.exec_fetchall(
                "SELECT column_name "
                + "FROM information_schema.columns "
                + "WHERE table_schema = 'public' "
                + f"AND table_name = '{table_name}';"
            )
            column_names = [row[0] for row in result]
            for column_name in column_names:
                table.add_column(Column(column_name))

            tables[table_name] = table

        return tables

    def store_indexable_columns(self, query, tables):
        for table_name in tables:
            if table_name in query.text:
                table = tables[table_name]
                for column in table.columns:
                    if column.name in query.text:
                        query.columns.append(column)

    def execute(self):
        file_path = os.path.dirname(os.path.abspath(__file__))
        query_files = glob.glob(
            f"{file_path}/../custom_workloads/{self.benchmark_name}/*.sql"
        )
        query_files.sort()

        # Retrieve schema to search for indexable columns
        tables = self.get_tables()

        queries = []

        for file_name in query_files:
            with open(file_name) as f:
                query_text = f.read()
                query_id = file_name.split("/")[-1]
                query = Query(query_id, query_text)
                self.store_indexable_columns(query, tables)
                queries.append(query)

        return Workload(queries)
