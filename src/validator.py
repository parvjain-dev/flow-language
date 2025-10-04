# src/validator.py

from lark import Visitor, Tree

class Validator(Visitor):
    def __init__(self, schemas, variable_schemas):
        self.schemas = schemas
        self.variable_schemas = variable_schemas
        self.current_schema = None
        self.current_schema_name = "unknown"
        self.group_by_cols = []

    def assignment(self, tree):
        new_var_name = tree.children[0].value
        rhs_node = tree.children[1]
        self.visit(rhs_node)
        if rhs_node.data == 'join_expr':
            left_source_name = rhs_node.children[0].value
            right_source_name = rhs_node.children[1].value
            left_schema_name = self.variable_schemas[left_source_name]
            right_schema_name = self.variable_schemas[right_source_name]
            merged_schema = self.schemas[left_schema_name].copy()
            merged_schema.update(self.schemas[right_schema_name])
            new_schema_name = f"join_{left_schema_name}_{right_schema_name}"
            self.schemas[new_schema_name] = merged_schema
            self.variable_schemas[new_var_name] = new_schema_name
        elif rhs_node.data == 'pipeline':
            start_var_name = rhs_node.children[0].value
            self.variable_schemas[new_var_name] = self.variable_schemas.get(start_var_name)

    # UPDATED: All join logic is now self-contained in this single method.
    def join_expr(self, tree):
        left_source_token, right_source_token, join_condition_node = tree.children
        left_source_name = left_source_token.value
        right_source_name = right_source_token.value

        # 1. Check that sources are valid
        for source in [left_source_name, right_source_name]:
            if source not in self.variable_schemas:
                raise ValueError(f"Validation Error: Variable '{source}' used in join is not defined or has no schema.")
        
        # 2. Build a local context for this join
        context_schemas = {
            left_source_name: self.schemas[self.variable_schemas[left_source_name]],
            right_source_name: self.schemas[self.variable_schemas[right_source_name]]
        }

        # 3. Directly validate the join condition
        left_col_ref = join_condition_node.children[0]
        right_col_ref = join_condition_node.children[1]

        for col_ref in [left_col_ref, right_col_ref]:
            table_prefix = col_ref.children[0].value
            column_name = col_ref.children[1].value

            if table_prefix not in context_schemas:
                raise ValueError(f"Validation Error: Unknown table alias '{table_prefix}' in join condition.")
            if column_name not in context_schemas[table_prefix]:
                raise ValueError(f"Validation Error: Column '{column_name}' not found in schema for '{table_prefix}'.")

    def pipeline(self, tree):
        start_var = tree.children[0].value
        schema_name = self.variable_schemas.get(start_var)
        previous_schema, previous_schema_name = self.current_schema, self.current_schema_name
        self.group_by_cols = []
        if schema_name:
            self.current_schema = self.schemas.get(schema_name, {}).copy()
            self.current_schema_name = schema_name
        else:
            self.current_schema, self.current_schema_name = None, "unknown"
        for child in tree.children:
            if isinstance(child, Tree): self.visit(child)
        self.current_schema, self.current_schema_name = previous_schema, previous_schema_name

    def column_ref(self, tree):
        # This generic check should NOT run during a join, as the context is different.
        # The join_expr method now handles its own column validation.
        if self.current_schema is None: return

        # This check is for columns inside a pipeline (filter, mutate)
        col_name = tree.children[1].value
        if col_name not in self.current_schema:
            raise ValueError(f"Validation Error: Column '{col_name}' not found in schema '{self.current_schema_name}'.")

    # ... (rest of the file is unchanged, and the old join_condition method is gone) ...
    def group_by(self, tree):
        if not self.current_schema: return
        self.group_by_cols = []
        for col_name_node in tree.children:
            col_name = col_name_node.value
            if col_name not in self.current_schema: raise ValueError(f"Validation Error: Column '{col_name}' in group_by not found in schema '{self.current_schema_name}'.")
            self.group_by_cols.append(col_name)

    def aggregate(self, tree):
        if not self.current_schema: return
        new_schema = {}
        for col in self.group_by_cols: new_schema[col] = self.current_schema.get(col, 'unknown')
        for agg_expr_node in tree.children:
            self.visit(agg_expr_node)
            new_col_name = agg_expr_node.children[0].value
            new_schema[new_col_name] = 'aggregate'
        self.current_schema = new_schema
        self.current_schema_name = "dynamic_aggregate"
        
    def agg_expr(self, tree):
        for child in tree.children:
            if isinstance(child, Tree): self.visit(child)
            
    def agg_function(self, tree):
        if not self.current_schema: return
        for token in tree.children:
            if token.type == 'NAME':
                col_name = token.value
                if col_name not in self.current_schema: raise ValueError(f"Validation Error: Column '{col_name}' in aggregate function not found in schema '{self.current_schema_name}'.")

    def mutate(self, tree):
        if self.current_schema is None: return
        for mutate_expr_node in tree.children:
            if isinstance(mutate_expr_node, Tree) and len(mutate_expr_node.children) > 1: self.visit(mutate_expr_node.children[1])
            new_col_name = mutate_expr_node.children[0].value
            self.current_schema[new_col_name] = 'dynamic'

    def arith_expr(self, tree):
        for child in tree.children:
            if isinstance(child, Tree): self.visit(child)
    def term(self, tree):
        for child in tree.children:
            if isinstance(child, Tree): self.visit(child)
    def factor(self, tree):
        for child in tree.children:
            if isinstance(child, Tree): self.visit(child)
    def mutate_expr(self, tree):
        for child in tree.children:
            if isinstance(child, Tree): self.visit(child)
    def bool_expression(self, tree):
        for child in tree.children:
            if isinstance(child, Tree): self.visit(child)

    def select(self, tree):
        if not self.current_schema: return
        for col_name_node in tree.children:
            col_name = col_name_node.value
            if col_name not in self.current_schema: raise ValueError(f"Validation Error: Column '{col_name}' not found in schema '{self.current_schema_name}'.")

    def get_current_schema_name(self):
        return self.current_schema_name