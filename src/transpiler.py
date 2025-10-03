# src/transpiler.py

from lark import Transformer, v_args

class FlowTranspiler(Transformer):
    def __init__(self):
        super().__init__()
        self.code_blocks = []
        self.sinks = {}
        self.variables = {} 
        # NEW: "Memory" for schemas and linking variables to them
        self.schemas = {}
        self.variable_schemas = {}
        self.temp_var_count = 0

    def _new_temp_var(self): # ... (this method is unchanged)
        self.temp_var_count += 1
        return f"temp_df_{self.temp_var_count}"

    # --- Methods for grammar rules ---

    def NAME(self, n): return n.value
    def STRING(self, s): return s[1:-1] # ... (rest of the simple handlers are unchanged) ...
    def SIGNED_NUMBER(self, n): return int(n)
    def OPERATOR(self, op): return op.value
    def TYPE(self, t): return t.value
    @v_args(inline=True)
    def op(self, o): return o.value
    @v_args(inline=True)
    def value(self, v): return v
    @v_args(inline=True)
    def pipe_step(self, item): return item
    @v_args(inline=True)
    def transformation(self, t): return t

    def arguments(self, args): return {args[0]: args[1]}
    def function_call(self, fc): return {"name": fc[0], "args": fc[1]}
    
    # NEW: A handler for individual fields in a schema
    def field_decl(self, f):
        field_name, field_type = f
        return (field_name, field_type)

    # NEW: A handler for the whole schema block
    def schema_decl(self, s):
        schema_name, *fields = s
        self.schemas[schema_name] = dict(fields)
        # This rule doesn't generate any Python code, it just stores the schema
        return None # Return None to prevent it from being processed further

    def condition(self, c): # ... (this method is unchanged) ...
        col, op, val = c[1], c[2], c[3]
        if isinstance(val, str): val = f"'{val}'"
        return f"({{df}}['{col}'] {op} {val})"

    def expression(self, e): # ... (this method is unchanged) ...
        query = e[0]
        for i in range(1, len(e), 2):
            op, cond = e[i], e[i+1]
            if op == 'and': query += f" & {cond}"
            elif op == 'or': query += f" | {cond}"
        return query

    def filter(self, f): return ('filter', f[0])
    def select(self, s): return ('select', [name for name in s])

    # UPDATED: source_decl now understands the 'using' clause
    def source_decl(self, s):
        flow_var, func_call, *schema_name_list = s
        python_var = f"{flow_var}_df"
        self.variables[flow_var] = python_var
        
        # Link variable to a schema if 'using' is present
        if schema_name_list:
            schema_name = schema_name_list[0]
            if schema_name in self.schemas:
                self.variable_schemas[flow_var] = schema_name
            else:
                raise Exception(f"Error: Schema '{schema_name}' not defined.")

        if func_call['name'] == 'File':
            path = func_call['args']['path']
            line = f"{python_var} = pd.read_csv('{path}')"
            self.code_blocks.append(line)

    def sink_decl(self, s): # ... (this method is unchanged) ...
        name, func_call = s[0], s[1]
        self.sinks[name] = func_call

    def pipeline(self, p): # ... (this method is unchanged) ...
        start_flow_var = p[0]
        start_py_var = self.variables.get(start_flow_var)
        if not start_py_var:
            raise Exception(f"Error: Variable '{start_flow_var}' not defined.")
        code = []
        current_py_var = start_py_var
        for item in p[1:]:
            next_py_var = self._new_temp_var()
            if isinstance(item, tuple):
                op_type, op_args = item
                if op_type == 'filter':
                    condition_str = op_args.format(df=current_py_var)
                    code.append(f"{next_py_var} = {current_py_var}[{condition_str}]")
                elif op_type == 'select':
                    code.append(f"{next_py_var} = {current_py_var}[{op_args}]")
                current_py_var = next_py_var
            elif isinstance(item, str):
                sink_name = item
                sink_info = self.sinks.get(sink_name)
                if sink_info and sink_info['name'] == 'File':
                    path = sink_info['args']['path']
                    code.append(f"{current_py_var}.to_csv('{path}', index=False)")
        return (code, current_py_var)

    def assignment(self, a): # ... (this method is unchanged) ...
        flow_var, pipeline_result = a[0], a[1]
        pipeline_code, last_py_var = pipeline_result
        new_py_var = f"{flow_var}_df"
        self.variables[flow_var] = new_py_var
        pipeline_code.append(f"{new_py_var} = {last_py_var}")
        comment = f"# Pipeline for '{flow_var}'"
        self.code_blocks.append(f"\n{comment}\n" + "\n".join(pipeline_code))
        
    def execution(self, e): # ... (this method is unchanged) ...
        pipeline_result = e[0]
        pipeline_code, _ = pipeline_result
        comment = f"# Standalone pipeline execution"
        self.code_blocks.append(f"\n{comment}\n" + "\n".join(pipeline_code))

    def start(self, s):
        header = "import pandas as pd\n"
        # Filter out None values from schema declarations
        final_blocks = [b for b in self.code_blocks if b is not None]
        body = "\n\n".join(final_blocks)
        return header + body