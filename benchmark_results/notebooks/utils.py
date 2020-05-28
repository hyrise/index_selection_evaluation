class Style():
    def __init__(self, label, color, marker, hatch):
        self.label = label
        self.color = color
        self.marker = marker
        self.hatch = hatch
        self.index_history = None

styles = {
    'epic': Style('Extend', '#4e79a7', 'd', '/'),
    'drop_heuristic': Style('Drop', '#f28e2b', 'P', 'o'),
    'microsoft': Style('AutoAdmin', '#e15759', '.', '.'),
    'microsoft_naive_2': Style('Naive 2', '#9c755f', '.', ''),
    'dexter': Style('Dexter', '#bab0ac', 'X', 'x'),
    'ibm': Style('DB2Advis', '#59a14f', '*', '*'),
    'no_index': Style('No Index', '#76b7b2', '-', 'O'),
    'cophy': Style('CoPhy', '#b07aa1', 'p', '\\'),
    'relaxation': Style('Relaxation', '#9c755f', 7, '-'),
    'reinforcement_learning': Style('Deep RL', '#ff9da7', '1', '+')
}
# Colors of color palette still available edc948, ff9da7

def b_to_gb(b):
    return b / 1000 / 1000 / 1000

def mb_to_gb(mb):
    return mb / 1000

def s_to_m(s):
    return s / 60