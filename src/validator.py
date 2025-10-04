# src/validator.py

from lark import Visitor, Tree

class Validator(Visitor):
    def __init__(self, schemas, variable_schemas):
        self.schemas = schemas
        self.variable_schemas = variable_schemas
        self.current_schema = None
        self.current_schema_name = "unknown"

    def pipeline(self, tree):
        start_var = tree.children[0].value
        schema_name = self.variable_schemas.get(start_var)
        
        previous_schema = self.current_schema
        previous_schema_name = self.current_schema_name
        
        if schema_name:
            self.current_schema = self.schemas.get(schema_name, {}).copy()
            self.current_schema_name = schema_name
        else:
            self.current_schema = None
            self.current_schema_name = "unknown"

        for child in tree.children:
            if isinstance(child, Tree):
                self.visit(child)
        
        self.current_schema = previous_schema
        self.current_schema_name = previous_schema_name

    def column_ref(self, tree):
        if not self.current_schema: return
        
        col_name = tree.children[1].value
        if col_name not in self.current_schema:
            raise ValueError(f"Validation Error: Column '{col_name}' not found in schema '{self.current_schema_name}'.")

    # UPDATED: This method now uses a safer two-pass approach.
    def mutate(self, tree):
        if self.current_schema is None: return

        # --- PASS 1: Validate all expressions against the current schema ---
        for mutate_expr_node in tree.children:
            if isinstance(mutate_expr_node, Tree) and len(mutate_expr_node.children) > 1:
                # Visit the 'arith_expr' (the right side of the '=')
                self.visit(mutate_expr_node.children[1])

        # --- PASS 2: Add all new columns to the schema for subsequent steps ---
        for mutate_expr_node in tree.children:
            new_col_name = mutate_expr_node.children[0].value
            self.current_schema[new_col_name] = 'dynamic' # Assume dynamic type for now

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
            if col_name not in self.current_schema:
                raise ValueError(f"Validation Error: Column '{col_name}' not found in schema '{self.current_schema_name}'.")