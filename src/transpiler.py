# src/transpiler.py

from lark import Transformer, v_args

class FlowTranspiler(Transformer):
    def __init__(self):
        super().__init__()
        # ... (init variables are unchanged) ...
        self.code_blocks = []
        self.sinks = {}
        self.variables = {} 
        self.schemas = {}
        self.variable_schemas = {}
        self.temp_var_count = 0
        self.imports = {"pandas as pd", "os"}

    def _new_temp_var(self): # ... (unchanged) ...
        self.temp_var_count += 1
        return f"temp_df_{self.temp_var_count}"

    # --- Basic Handlers (mostly unchanged) ---
    def NAME(self, n): return n.value
    def STRING(self, s): return s # Keep quotes for python code
    def SIGNED_NUMBER(self, n): return n.value
    def BOOL_OPERATOR(self, op): return op.value
    @v_args(inline=True)
    def op(self, o): return o.value
    @v_args(inline=True)
    def pipe_step(self, item): return item
    @v_args(inline=True)
    def transformation(self, t): return t
    # ... (other simple handlers are unchanged) ...
    def field_decl(self, f): return (f[0], f[1].value)
    def env_var(self, e): return ('env', e[0][1:-1])
    def arguments(self, args): return {args[i]: args[i+1] for i in range(0, len(args), 2)}
    def function_call(self, fc): return {"name": fc[0], "args": fc[1] if len(fc) > 1 else {}}
    def schema_decl(self, s):
        schema_name, *fields = s
        self.schemas[schema_name] = dict(fields)
        return None

    # --- Expression Handlers (NEW & UPDATED) ---
    @v_args(inline=True)
    def column_ref(self, table, column):
        # We'll use a placeholder that the pipeline resolves
        return f"{{df}}['{column}']"

    # These methods recursively build the python expression string
    def arith_expr(self, items):
        return f"({' '.join(items)})"
    def term(self, items):
        return ' '.join(items)
    def factor(self, items):
        return items[0]

    def bool_expression(self, items):
        # Join multiple conditions with '&' for pandas
        return " & ".join(f"({item})" for item in items)
    
    def filter(self, f):
        return ('filter', f[0])

    def select(self, s): return ('select', [name for name in s])
    def sort(self, s): # ... (unchanged) ...
        columns = []
        order = 'asc'
        for item in s:
            if isinstance(item, str) and item in ('asc', 'desc'): order = item
            else: columns.append(item)
        ascending = (order == 'asc')
        return ('sort', {'by': columns, 'ascending': ascending})

    # NEW: Handler for mutate
    def mutate_expr(self, m):
        return {m[0]: m[1]} # Returns a dict {new_col_name: expression_string}
    
    def mutate(self, m):
        all_mutations = {}
        for item in m:
            all_mutations.update(item)
        return ('mutate', all_mutations)
        
    # --- Core Logic (source_decl, pipeline, etc.) ---
    def source_decl(self, s): # ... (unchanged) ...
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
        elif func_call['name'] == 'Postgres':
            self.imports.add("create_engine from sqlalchemy")
            args = func_call['args']
            password = args['password']
            if isinstance(password, tuple) and password[0] == 'env': password_py_expr = f"os.getenv('{password[1]}')"
            else: password_py_expr = f"'{password}'"
            conn_str = f"'postgresql+psycopg2://{args['user']}:' + {password_py_expr} + '@{args['host']}/{args['database'][1:-1]}'"
            code = f"""engine = create_engine({conn_str})\n{python_var} = pd.read_sql_table('{args['table'][1:-1]}', engine)"""
            self.code_blocks.append(code)

    def sink_decl(self, s): # ... (unchanged) ...
        name, func_call = s[0], s[1]
        self.sinks[name] = func_call

    def pipeline(self, p): # ... (UPDATED to handle mutate) ...
        start_flow_var = p[0]
        start_py_var = self.variables.get(start_flow_var)
        if not start_py_var: raise Exception(f"Error: Variable '{start_flow_var}' not defined.")
        
        code = []
        current_py_var = start_py_var
        
        for item in p[1:]:
            next_py_var = self._new_temp_var()
            if isinstance(item, tuple):
                op_type, op_args = item
                if op_type == 'filter':
                    condition_str = op_args.replace("{df}", current_py_var)
                    code.append(f"{next_py_var} = {current_py_var}[{condition_str}]")
                elif op_type == 'select':
                    code.append(f"{next_py_var} = {current_py_var}[{op_args}]")
                elif op_type == 'sort':
                    code.append(f"{next_py_var} = {current_py_var}.sort_values(by={op_args['by']}, ascending={op_args['ascending']})")
                elif op_type == 'mutate':
                    # Use pandas.assign for clean, chained column creation
                    assign_args = ", ".join([f"{k} = {v.replace('{df}', current_py_var)}" for k, v in op_args.items()])
                    code.append(f"{next_py_var} = {current_py_var}.assign({assign_args})")
                current_py_var = next_py_var
            elif isinstance(item, str):
                sink_name = item
                sink_info = self.sinks.get(sink_name)
                if sink_info and sink_info['name'] == 'File':
                    path = sink_info['args']['path'][1:-1]
                    code.append(f"{current_py_var}.to_csv('{path}', index=False)")
        
        return (code, current_py_var)
    # ... (assignment, execution, start methods are unchanged) ...
    def assignment(self, a):
        flow_var, pipeline_result = a[0], a[1]
        pipeline_code, last_py_var = pipeline_result
        new_py_var = f"{flow_var}_df"
        self.variables[flow_var] = new_py_var
        pipeline_code.append(f"{new_py_var} = {last_py_var}")
        comment = f"# Pipeline for '{flow_var}'"
        self.code_blocks.append(f"\n{comment}\n" + "\n".join(pipeline_code))
    def execution(self, e):
        pipeline_result = e[0]
        pipeline_code, _ = pipeline_result
        comment = f"# Standalone pipeline execution"
        self.code_blocks.append(f"\n{comment}\n" + "\n".join(pipeline_code))
    def start(self, s):
        import_statements = [f"import {imp}" if " from " not in imp else f"from {imp.split(' from ')[1]} import {imp.split(' from ')[0]}" for imp in sorted(list(self.imports))]
        header = "\n".join(import_statements)
        final_blocks = [b.strip() for b in self.code_blocks if b is not None]
        body = "\n\n".join(final_blocks)
        return header + "\n\n" + body