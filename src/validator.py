# src/validator.py

from lark import Visitor, Tree

class Validator(Visitor):
    def __init__(self, schemas, variable_schemas):
        self.schemas = schemas
        self.variable_schemas = variable_schemas
        self.current_schema = None
        self.current_schema_name = "unknown"
        self.group_by_cols = []

    def pipeline(self, tree):
        start_var = tree.children[0].value
        schema_name = self.variable_schemas.get(start_var)
        
        previous_schema = self.current_schema
        previous_schema_name = self.current_schema_name
        self.group_by_cols = []
        
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

    def group_by(self, tree):
        if not self.current_schema: return
        self.group_by_cols = []
        for col_name_node in tree.children:
            col_name = col_name_node.value
            if col_name not in self.current_schema:
                raise ValueError(f"Validation Error: Column '{col_name}' in group_by not found in schema '{self.current_schema_name}'.")
            self.group_by_cols.append(col_name)

    def aggregate(self, tree):
        if not self.current_schema: return
        new_schema = {}
        for col in self.group_by_cols:
            new_schema[col] = self.current_schema.get(col, 'unknown')
        for agg_expr_node in tree.children:
            self.visit(agg_expr_node)
            new_col_name = agg_expr_node.children[0].value
            new_schema[new_col_name] = 'aggregate'
        self.current_schema = new_schema
        self.current_schema_name = "dynamic_aggregate"
        
    def agg_expr(self, tree):
        for child in tree.children:
            if isinstance(child, Tree): self.visit(child)
            
    # UPDATED: This method is now smarter and checks token types.
    def agg_function(self, tree):
        if not self.current_schema: return

        # Iterate through children and only validate the column NAME, not the function name.
        for token in tree.children:
            if token.type == 'NAME':
                col_name = token.value
                if col_name not in self.current_schema:
                    raise ValueError(f"Validation Error: Column '{col_name}' in aggregate function not found in schema '{self.current_schema_name}'.")

    def column_ref(self, tree):
        if not self.current_schema: return
        col_name = tree.children[1].value
        if col_name not in self.current_schema:
            raise ValueError(f"Validation Error: Column '{col_name}' not found in schema '{self.current_schema_name}'.")

    def mutate(self, tree):
        if self.current_schema is None: return
        for mutate_expr_node in tree.children:
            if isinstance(mutate_expr_node, Tree) and len(mutate_expr_node.children) > 1:
                self.visit(mutate_expr_node.children[1])
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
            if col_name not in self.current_schema:
                raise ValueError(f"Validation Error: Column '{col_name}' not found in schema '{self.current_schema_name}'.")

    def get_current_schema_name(self):
        return self.current_schema_name