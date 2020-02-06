from functools import total_ordering

@total_ordering
class Index:
    def __init__(self, columns):
        if len(columns) == 0:
            raise ValueError('Index needs at least 1 column')
        self.columns = tuple(columns)
        # Store hypopg estimated size when `store_size=True` (whatif)
        self.estimated_size = None
        self.hypopg_name = None

    # Used to sort indexes
    def __lt__(self, other):
        # print(str(self.columns))
        # print(str(other.columns))
        # return str(self.columns) < str(other.columns)
        if len(self.columns) != len(other.columns):
            return len(self.columns) < len(other.columns)

        return self.columns < other.columns

    def __repr__(self):
        columns_string = ','.join(map(str, self.columns))
        return f'I({columns_string})'

    def __eq__(self, other):
        return self.columns == other.columns

    def __hash__(self):
        return hash(self.columns)

    def _column_names(self):
        return [x.name for x in self.columns]

    def is_single_column(self):
        return True if len(self.columns) == 1 else False

    def table(self):
        return self.columns[0].table

    def index_idx(self):
        columns = '_'.join(self._column_names())
        return f'{self.table()}_{columns}_idx'

    def joined_column_names(self):
        return ','.join(self._column_names())

    def appendable_by(self, other):
        if self.table() != other.table():
            return False

        if not other.is_single_column():
            return False

        if other.columns[0] in self.columns:
            return False

        return True
