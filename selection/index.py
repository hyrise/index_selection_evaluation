class Index:
    def __init__(self, columns):
        if len(columns) == 0:
            raise ValueError('Index needs at least 1 column')
        elif len(columns) == 1:
            self.multicolumn = False
        else:
            self.multicolumn = True
        self.singlecolumn = not self.multicolumn
        self.columns = columns
        # Store hypopg estimated size when `store_size=True` (whatif)
        self.estimated_size = None
        self.hypopg_name = None

    # Used to sort indexes
    def __lt__(self, other):
        return str(self.columns) < str(other.columns)

    def __add__(self, other):
        return Index(self.columns + other.columns)

    def __str__(self):
        columns_string = ','.join(map(str, self.columns))
        return f'I({columns_string})'

    def __repr__(self):
        columns_string = ','.join(map(str, self.columns))
        return f'I({columns_string})'

    def _column_names(self):
        return [x.name for x in self.columns]

    def index_idx(self):
        table = self.columns[0].table
        columns = '_'.join(self._column_names())
        return f'{table}_{columns}_idx'

    def joined_column_names(self):
        return ','.join(self._column_names())

    def appendable_by(self, other):
        if self.columns[0].table == other.columns[0].table and \
           other.columns[0] not in self.columns and \
           len(other.columns) == 1:
            return True
        return False
