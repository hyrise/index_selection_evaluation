from .index import Index
import logging


class Workload:
    def __init__(self, queries, database_name):
        # TODO add indexable_column function
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

        self._id = identifier

    def __lt__(self, other):
        return self.name < other.name

    def __str__(self):
        return 'C{} {}'.format(self._id, self.name)

    def __repr__(self):
        return str(self)


class Table:
    def __init__(self, name):
        self.name = name.lower()
        self.columns = []

    def __str__(self):
        return self.name


class Query:
    def __init__(self, query_id, query_text, database_connector):
        self.nr = query_id
        self.text = query_text.lower()
        self.db_connector = database_connector
        self.columns = []

        self.text = self.db_connector.update_query_text(self.text)
        self._retrieve_columns()

    def _retrieve_columns(self):
        #  print(self.text)
        if not self.db_connector:
            logging.info('{}:'.format(self))
            logging.info('No database connector to get indexable columns')
            return
        try:
            self.columns = self.db_connector.indexable_columns(self)
            self.columns = sorted(self.columns)
            logging.debug("#columns ({}): {}".format(self,
                                                     len(self.columns)))
        except Exception as e:
            self.db_connector.rollback()
            logging.error('{}: {}'.format(self, e))

    def __str__(self):
        return 'Q{}'.format(self.nr)
