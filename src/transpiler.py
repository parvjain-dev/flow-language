# src/transpiler.py

from lark import Transformer, v_args

class FlowTranspiler(Transformer):
    def __init__(self):
        self.code_blocks = []
        self.sinks = {}
        self.variables = {} 
        self.schemas = {}
        self.variable_schemas = {}
        self.temp_var_count = 0
        self.imports = {"pandas as pd", "os"}

    def _new_temp_var(self):
        self.temp_var_count += 1
        return f"temp_df_{self.temp_var_count}"

    # --- Methods for grammar rules ---
    
    def NAME(self, n): return n.value
    def STRING(self, s): return s
    def SIGNED_NUMBER(self, n): return n.value
    def BOOL_OPERATOR(self, op): return op.value
    def AGG_FUNC_NAME(self, n): return n.value
    def TYPE(self, t): return t.value
    @v_args(inline=True)
    def op(self, o): return o.value
    @v_args(inline=True)
    def pipe_step(self, item): return item
    @v_args(inline=True)
    def transformation(self, t): return t
    def field_decl(self, f): return (f[0], f[1])
    def env_var(self, e): return ('env', e[0][1:-1])
    def arguments(self, args): return {args[i]: args[i+1] for i in range(0, len(args), 2)}
    def function_call(self, fc): return {"name": fc[0], "args": fc[1] if len(fc) > 1 else {}}
    def schema_decl(self, s):
        schema_name, *fields = s
        self.schemas[schema_name] = dict(fields)
        return None
    @v_args(inline=True)
    def column_ref(self, table, column): return (table, column)
    def arith_expr(self, items):
        s = str(items[0])
        for i in range(1, len(items), 2):
            op = str(items[i])
            val = str(items[i+1])
            s += f" {op} {val}"
        return f"({s})"
    def term(self, items):
        s = str(items[0])
        for i in range(1, len(items), 2):
            op = str(items[i])
            val = str(items[i+1])
            s += f" {op} {val}"
        return s
    def factor(self, items):
        if isinstance(items[0], tuple):
            table, column = items[0]
            return f"{{df}}['{column}']"
        return str(items[0])
    def bool_expression(self, items):
        s = str(items[0])
        for i in range(1, len(items), 2):
            op = str(items[i])
            val = str(items[i+1])
            s += f" {op} {val}"
        return s
    
    def assert_statement(self, a):
        condition_str = a[0]
        final_condition = condition_str.replace("{df}", "df")
        self.code_blocks.append(f"assert {final_condition}")
        return None

    def filter(self, f): return ('filter', f[0])
    def select(self, s): return ('select', [name for name in s])
    def sort(self, s):
        columns, order = [], 'asc'
        for item in s:
            if item in ('asc', 'desc'): order = item
            else: columns.append(item)
        return ('sort', {'by': columns, 'ascending': (order == 'asc')})
    def mutate_expr(self, m): return {m[0]: m[1]}
    def mutate(self, m):
        all_mutations = {}
        for item in m: all_mutations.update(item)
        return ('mutate', all_mutations)
    def group_by(self, g):
        return ('group_by', [name for name in g])
    def agg_function(self, a):
        func_name, *col_list = a
        col_name = col_list[0] if col_list else None
        pandas_func_map = {'avg': 'mean'}
        pandas_func = pandas_func_map.get(func_name, func_name)
        return (col_name, pandas_func)
    def agg_expr(self, a):
        new_col_name, agg_func_tuple = a
        return (new_col_name, agg_func_tuple)
    def aggregate(self, a):
        agg_dict = {new_col: (col, func) for new_col, (col, func) in a}
        return ('aggregate', agg_dict)
    def join_condition(self, j):
        left_col_ref, right_col_ref = j
        return {'left_on': left_col_ref[1], 'right_on': right_col_ref[1]}
    def join_expr(self, j):
        left_source, right_source, condition_result = j
        left_df, right_df = self.variables.get(left_source), self.variables.get(right_source)
        left_on, right_on = condition_result['left_on'], condition_result['right_on']
        new_py_var = self._new_temp_var()
        code_line = f"{new_py_var} = pd.merge({left_df}, {right_df}, left_on='{left_on}', right_on='{right_on}')"
        return ([code_line], new_py_var)
    def source_decl(self, s):
        flow_var, func_call, *schema_name_list = s
        python_var = f"{flow_var}_df"
        self.variables[flow_var] = python_var
        if schema_name_list:
            schema_name = schema_name_list[0]
            if schema_name in self.schemas: self.variable_schemas[flow_var] = schema_name
            else: raise Exception(f"Error: Schema '{schema_name}' not defined.")
        if func_call['name'] == 'File':
            path = func_call['args']['path'][1:-1]
            self.code_blocks.append(f"{python_var} = pd.read_csv('{path}')")
        elif func_call['name'] == 'Parquet':
            path = func_call['args']['path'][1:-1]
            self.code_blocks.append(f"{python_var} = pd.read_parquet('{path}')")
        elif func_call['name'] == 'Postgres':
            self.imports.add("create_engine from sqlalchemy")
            args = func_call['args']
            password = args['password']
            if isinstance(password, tuple) and password[0] == 'env':
                env_var_name = password[1]
                user, host = args['user'][1:-1], args['host'][1:-1]
                database, table = args['database'][1:-1], args['table'][1:-1]
                code = f"""
password = os.getenv('{env_var_name}')
if password is None:
    raise ValueError("Flow Execution Error: Environment variable '{env_var_name}' for the database password is not set.")
conn_str = f'postgresql+psycopg2://{user}:{{password}}@{host}/{database}'
engine = create_engine(conn_str)
{python_var} = pd.read_sql_table('{table}', engine)
"""
                self.code_blocks.append(code.strip())
    def sink_decl(self, s):
        name, func_call = s[0], s[1]
        self.sinks[name] = func_call
    def pipeline(self, p):
        start_flow_var, *steps = p
        start_py_var = self.variables.get(start_flow_var)
        if not start_py_var: raise Exception(f"Error: Variable '{start_flow_var}' not defined.")
        code, current_py_var, is_grouped = [], start_py_var, False
        self.group_by_cols = [] 
        for item in steps:
            next_py_var = self._new_temp_var()
            if isinstance(item, tuple):
                op_type, op_args = item
                if op_type == 'filter': code.append(f"{next_py_var} = {current_py_var}[{op_args.replace('{df}', current_py_var)}]")
                elif op_type == 'select': code.append(f"{next_py_var} = {current_py_var}[{op_args}]")
                elif op_type == 'sort': code.append(f"{next_py_var} = {current_py_var}.sort_values(by={op_args['by']}, ascending={op_args['ascending']})")
                elif op_type == 'mutate':
                    assign_args = ", ".join([f"{k}={v.replace('{df}', current_py_var)}" for k,v in op_args.items()])
                    code.append(f"{next_py_var} = {current_py_var}.assign({assign_args})")
                elif op_type == 'group_by':
                    self.group_by_cols = op_args
                    code.append(f"{next_py_var} = {current_py_var}.groupby({op_args})")
                    is_grouped = True
                elif op_type == 'aggregate':
                    if not is_grouped: raise Exception("Error: 'aggregate' must be preceded by a 'group_by'.")
                    agg_args_list = []
                    for new_col, (src_col, func) in op_args.items():
                        col_for_count = self.group_by_cols[0] if self.group_by_cols else 'UNKNOWN_COLUMN'
                        agg_col = src_col if src_col is not None else col_for_count
                        agg_args_list.append(f"{new_col}=pd.NamedAgg(column='{agg_col}', aggfunc='{func}')")
                    agg_args = ", ".join(agg_args_list)
                    code.append(f"{next_py_var} = {current_py_var}.agg({agg_args}).reset_index()")
                    is_grouped = False
                current_py_var = next_py_var
            elif isinstance(item, str):
                sink_name = item
                sink_info = self.sinks.get(sink_name)
                if sink_info and sink_info['name'] == 'File':
                    path = sink_info['args']['path'][1:-1]
                    code.append(f"{current_py_var}.to_csv('{path}', index=False)")
                elif sink_info and sink_info['name'] == 'Parquet':
                    path = sink_info['args']['path'][1:-1]
                    code.append(f"{current_py_var}.to_parquet('{path}', index=False)")
        return (code, current_py_var)

    def assignment(self, a):
        flow_var, expression_result = a
        pipeline_code, last_py_var = expression_result
        new_py_var = f"{flow_var}_df"
        self.variables[flow_var] = new_py_var
        if pipeline_code:
            pipeline_code.append(f"{new_py_var} = {last_py_var}")
        comment = f"# Pipeline for '{flow_var}'"
        self.code_blocks.append(f"\n{comment}\n" + "\n".join(pipeline_code))
        
    def execution(self, e):
        pipeline_result, = e
        pipeline_code, _ = pipeline_result
        comment = f"# Standalone pipeline execution"
        self.code_blocks.append(f"\n{comment}\n" + "\n".join(pipeline_code))

    # UPDATED: This is the corrected 'start' method
    def start(self, s):
        # The child nodes in 's' have already been transformed, and their methods
        # (like assert_statement) have populated self.code_blocks.
        # This method's only job is to assemble the final script.
        import_statements = sorted(list(self.imports), key=lambda x: " from " in x)
        header = "\n".join(f"import {imp}" if " from " not in imp else f"from {imp.split(' from ')[1]} import {imp.split(' from ')[0]}" for imp in import_statements)
        
        final_blocks = [b.strip() for b in self.code_blocks if b]
        body = "\n\n".join(final_blocks)

        return header + "\n\n" + body