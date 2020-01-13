import sys
import statistics
import json


# Example how to run:
# python3 csv_to_tikz.py ../benchmark_results/range_results_14_queries.csv
# plot1.tex
TOP = r"""\documentclass[tikz]{standalone}
\usepackage{tikz}
\usepackage{pgfplots}
\pgfplotsset{compat=newest}
\definecolor{s1}{RGB}{215, 25, 28}
\definecolor{s2}{RGB}{253, 174, 97}
\definecolor{s3}{RGB}{92, 89, 83}
\definecolor{s4}{RGB}{171, 221, 164}
\definecolor{s5}{RGB}{43, 131, 186}
\definecolor{s6}{RGB}{182, 68, 184}
\pgfplotscreateplotcyclelist{set1}{
    s1,every mark/.append style={fill=s1,fill opacity=0.1},mark=square*\\
    s2,every mark/.append style={fill=s2,fill opacity=0.1},mark=o\\
    s3,every mark/.append style={fill=s3,fill opacity=0.1},mark=triangle*\\
    s4,every mark/.append style={fill=s4,fill opacity=0.1},mark=diamond*\\
    s5,every mark/.append style={fill=s5,fill opacity=0.1},mark=star\\
}

\begin{document}
\pgfkeys{/pgf/number format/.cd,1000 sep={}}
\begin{tikzpicture}
\begin{axis}[height=9cm, width=\textwidth, legend
    style={at={(1,1)},anchor=north east}, xlabel={Storage Consumption [MB]},
    ylabel={Query Workload Costs},
    scaled y ticks=false, ymode=log,
    every axis plot/.append style={thick},
    cycle list name=set1
    ]
"""
#  MID = r"""
#  \end{axis}
#  \begin{axis}[height=10cm, width=14cm, ylabel near ticks,
#      yticklabel pos=right, legend
#      style={at={(1,1)},anchor=south east}, xtick=\empty, ylabel={Size in
#      Gigabytes},enlargelimits=0.05,
#      scaled y ticks=false]
#  """
BOT = r"""
\end{axis}
\end{tikzpicture}
\end{document}
"""


class Attribute:
    def __init__(self, index, name, header=None, switchaxis=False,
                 options=None):
        self.index = index
        self.name = name
        self.values = []
        self.x_values = []
        self.indexes = []
        self.switchaxis = switchaxis
        self.options = options

        if header:
            for i in range(len(header)):
                if header[i][0] == 'q':
                    self.indexes.append(i)

    def append(self, line, x_attr_value):
        self.x_values.append(x_attr_value)
        value = self.append_workload_runtime(line)
        self.values.append(value)

    def append_workload_runtime(self, line):
        runtime = 0
        # ALGOTIME:
        # runtime = json.loads(line[7])
        for i in self.indexes:
            # COST:
            # runtime += json.loads(line[i])['Cost']
            # # query id 4 == 15
            # if i in [36, 68, 46, 15, 17, 85, 22, 21, 25, 58, 92]:
            #     continue
            try:
                runtimes = json.loads(line[i])['Runtimes']
                #  div = len(runtimes) * 1000
                # continue
                # RUNTIMES:
                # try:
                # runtime += sum(runtimes) / div
                tt = statistics.median(runtimes) / 1000
                # if tt > 10:
                #     print(i, tt)
                runtime += tt
            except Exception:
                print(i)
        return runtime

    def string(self):
        string = '\\addplot'
        if self.options:
            string += f'[{self.options}]'
        string += ' coordinates {\n'
        for i in range(len(self.values)):
            string += f'({self.x_values[i]},{self.values[i]})\n'
        string += '};\n\\addlegendentry{'
        string += self.name
        string += '}\n\n'

        return string


class TikzPlot:
    def __init__(self, csv_file, plot_name, attribute_name=None):
        self.csv_file = csv_file
        self.plot_name = plot_name
        self.attribute_name = attribute_name
        self.prev_value = None
        self.output_string = TOP

        self.read_file()

    def read_file(self):
        with open(self.csv_file, 'r') as file:
            # Replace for json syntax
            data = file.read().replace("'", '"')
        lines = data.split('\n')
        header = lines[0].split(';')
        lines = lines[1:]
        attributes = []

        prev_name = None

        for line in lines:
            if len(line) == 0:
                print('done')
                break
            line = line.replace('];[', '][')
            line = line.split(';')
            line[2] = line[2].replace('_', '')

            if not prev_name or prev_name != line[2]:
                attributes.append(Attribute(-1, line[2], header))
            prev_name = line[2]
            print(line[2])

            parameters = json.loads(line[3].replace('True',
                                                    'true').replace('False',
                                                                    'false'))
            value = json.loads(line[10]) / 1000000
            #  value = json.loads(line[8])
            if self.attribute_name in parameters:
                value = parameters[self.attribute_name]
            attributes[-1].append(line, value)

        for attribute in attributes:
            #  if attribute.switchaxis:
            #      self.output_string += MID
            self.output_string += attribute.string()
        self.output_string += BOT

    def store_tex(self):
        print('store')
        with open(self.plot_name, 'w') as file:
            file.write(self.output_string)


def main():
    plot = TikzPlot(sys.argv[1], sys.argv[2])
    plot.store_tex()


if __name__ == '__main__':
    main()
