class Index:
    def __init__(self, columns):
        if len(columns) == 0:
            raise ValueError('Index needs at least 1 column')
        self.columns = columns
        # Store hypopg estimated size when `store_size=True` (whatif)
        self.estimated_size = None
        self.hypopg_name = None

    # Used to sort indexes
    def __lt__(self, other):
        return str(self.columns) < str(other.columns)

    def __repr__(self):
        columns_string = ','.join(map(str, self.columns))
        return f'I({columns_string})'

    def _column_names(self):
        return [x.name for x in self.columns]

    def singlecolumn(self):
        return True if len(self.columns) == 1 else False

    def table(self):
        return self.columns[0].table

    def index_idx(self):
        columns = '_'.join(self._column_names())
        return f'{self.table()}_{columns}_idx'

    def joined_column_names(self):
        return ','.join(self._column_names())

    def appendable_by(self, other):
        if (self.table() == other.table() and
                len(other.columns) == 1 and
                other.columns[0] not in self.columns):
            return True
        return False
