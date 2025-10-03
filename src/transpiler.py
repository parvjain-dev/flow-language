# src/transpiler.py

from lark import Transformer, v_args

class FlowTranspiler(Transformer):
    def __init__(self):
        super().__init__()
        self.python_code_lines = []
        self.sinks = {}

    def NAME(self, n): return n.value
    def STRING(self, s): return s[1:-1]
    def SIGNED_NUMBER(self, n): return int(n)
    def OPERATOR(self, op): return op.value

    # NEW: This method handles our new 'op' rule from the grammar.
    @v_args(inline=True)
    def op(self, o):
        return o.value

    @v_args(inline=True)
    def value(self, v): return v
    @v_args(inline=True)
    def pipe_step(self, item): return item

    def arguments(self, args): return {args[0]: args[1]}
    def function_call(self, fc): return {"name": fc[0], "args": fc[1]}

    def condition(self, c):
        column, operator, value = c[1], c[2], c[3]
        if isinstance(value, str):
            value = f"'{value}'"
        return f"(df['{column}'] {operator} {value})"

    def expression(self, e):
        # Now, 'e' will correctly be a list like [condition1, 'and', condition2]
        parts = e
        query = parts[0]
        for i in range(1, len(parts), 2):
            op = parts[i]
            condition = parts[i+1]
            if op == 'and':
                query += f" & {condition}"
            elif op == 'or':
                query += f" | {condition}"
        return query

    def transformation(self, t):
        return t[0]

    def source_decl(self, s):
        # ... (rest of the file is unchanged) ...
        name, func_call = s[0], s[1]
        if func_call['name'] == 'File':
            path = func_call['args']['path']
            line = f"df = pd.read_csv('{path}')"
            self.python_code_lines.append(line)

    def sink_decl(self, s):
        name, func_call = s[0], s[1]
        self.sinks[name] = func_call

    def connection(self, c):
        chain = c
        for item in chain[1:]:
            if "df[" in str(item):
                filter_condition = item
                line = f"df = df[{filter_condition}]"
                self.python_code_lines.append(line)
            else:
                sink_name = item
                sink_info = self.sinks.get(sink_name)
                if sink_info and sink_info['name'] == 'File':
                    path = sink_info['args']['path']
                    line = f"df.to_csv('{path}', index=False)"
                    self.python_code_lines.append(line)

    def start(self, st):
        header = "import pandas as pd\n\n"
        body = "\n".join(self.python_code_lines)
        return header + body