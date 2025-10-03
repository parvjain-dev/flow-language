# src/validator.py

from lark import Visitor, Tree # Import Tree to check its type

class Validator(Visitor):
    def __init__(self, schemas, variable_schemas):
        self.schemas = schemas
        self.variable_schemas = variable_schemas
        self.current_schema = None

    def pipeline(self, tree):
        start_var = tree.children[0].value
        schema_name = self.variable_schemas.get(start_var)
        
        previous_schema = self.current_schema
        if schema_name:
            self.current_schema = self.schemas.get(schema_name)
        else:
            self.current_schema = None

        # UPDATED: We now check if the child is a Tree before visiting.
        for child in tree.children:
            if isinstance(child, Tree):
                self.visit(child)
        
        self.current_schema = previous_schema
    def sort(self, tree):
        if not self.current_schema:
            return # No schema to validate against
        
        # The children are the column names and optionally 'order' and a value
        for child in tree.children:
            # We only care about the column names, which are Tokens
            if not isinstance(child, Tree) and child.type == 'NAME':
                col_name = child.value
                schema_name = next((s_name for s_name, s_val in self.schemas.items() if s_val == self.current_schema), "unknown")
                if col_name not in self.current_schema:
                    raise ValueError(f"Validation Error: Column '{col_name}' not found in schema '{schema_name}' while trying to sort.")
    def select(self, tree):
        if not self.current_schema:
            return

        for col_name_node in tree.children:
            col_name = col_name_node.value
            # A more detailed error message
            schema_name = next((s_name for s_name, s_val in self.schemas.items() if s_val == self.current_schema), "unknown")
            if col_name not in self.current_schema:
                raise ValueError(f"Validation Error: Column '{col_name}' not found in schema '{schema_name}'.")
        
    def condition(self, tree):
        if not self.current_schema:
            return

        col_name = tree.children[1].value
        schema_name = next((s_name for s_name, s_val in self.schemas.items() if s_val == self.current_schema), "unknown")
        if col_name not in self.current_schema:
            raise ValueError(f"Validation Error: Column '{col_name}' not found in schema '{schema_name}'.")