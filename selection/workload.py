from .index import Index


class Workload:
    def __init__(self, queries, database_name):
        self.database_name = database_name
        self.queries = queries

    def indexable_columns(self):
        indexable_columns = []
        for query in self.queries:
            indexable_columns.extend(query.columns)
        return list(set(indexable_columns))


class Column:
    def __init__(self, identifier, name, table):
        self.name = name.lower()
        self.table = table
        self.single_column_index = Index([self])

    def __lt__(self, other):
        return self.name < other.name

    def __repr__(self):
        return f'C {self.table}.{self.name}'


class Table:
    def __init__(self, name):
        self.name = name.lower()
        self.columns = []

    def __repr__(self):
        return self.name


class Query:
    def __init__(self, query_id, query_text):
        self.nr = query_id
        self.text = query_text.lower()
        # Indexable columns
        self.columns = []

    def __repr__(self):
        return f'Q{self.nr}'
