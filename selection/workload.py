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

    def potential_indexes(self):
        return [Index([c]) for c in self.indexable_columns()]


class Column:
    def __init__(self, name):
        self.name = name.lower()
        self.table = None

    def __lt__(self, other):
        return self.name < other.name

    def __repr__(self):
        return f'C {self.table}.{self.name}'

    # We cannot check self.table == other.table here since Table.__eq__()
    # internally checks Column.__eq__. This would lead to endless recursions.
    def __eq__(self, other):
        if self.table is None and other.table is not None:
            return False

        if self.table is not None and other.table is None:
            return False

        if self.table is None and other.table is None:
            return self.name == other.name

        return self.table.name == other.table.name and self.name == other.name

    def __hash__(self):
        return hash((self.name, self.table.name))


class Table:
    def __init__(self, name):
        self.name = name.lower()
        self.columns = []

    def add_column(self, column):
        column.table = self
        self.columns.append(column)

        return column

    def add_columns(self, columns):
        for column in columns:
            _ = self.add_column(column)

        return columns

    def __repr__(self):
        return self.name

    def __eq__(self, other):
        return self.name == other.name and tuple(self.columns) == tuple(other.columns)

    def __hash__(self):
        return hash((self.name, tuple(self.columns)))


class Query:
    def __init__(self, query_id, query_text, columns = None):
        self.nr = query_id
        self.text = query_text.lower()

        # Indexable columns
        if columns is None:
            self.columns = []
        else:
            self.columns = columns
        

    def __repr__(self):
        return f'Q{self.nr}'
