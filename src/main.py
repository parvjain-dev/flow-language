# src/main.py

from lark import Lark
from transpiler import FlowTranspiler # <-- IMPORT our new class

# 1. Read the grammar file
with open("src/flow.lark", "r") as f:
    flow_grammar = f.read()

# 2. Create the Lark parser instance
flow_parser = Lark(flow_grammar, start='start')

# 3. Read the code we want to parse
# with open("examples/simple_copy.flow", "r") as f:
#     flow_code = f.read()
# with open("examples/filter_users.flow", "r") as f: # <-- CHANGE THIS FILENAME
#     flow_code = f.read()


# with open("examples/complex_filter.flow", "r") as f: # <-- CHANGE THIS
#     flow_code = f.read()
# with open("examples/select_users.flow", "r") as f: # <-- CHANGE THIS
#     flow_code = f.read()

# with open("examples/variables_test.flow", "r") as f: # <-- CHANGE THIS
#     flow_code = f.read()
with open("examples/schema_test.flow", "r") as f: # <-- CHANGE THIS
    flow_code = f.read()
# 4. Parse the code to get the tree
parse_tree = flow_parser.parse(flow_code)

# 5. Create an instance of our transpiler
transpiler = FlowTranspiler()

# 6. Transform the tree to generate Python code
python_script = transpiler.transform(parse_tree)

# 7. Print the final, generated script!
print("--- Generated Python Script ---")
print(python_script)