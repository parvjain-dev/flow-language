# src/main.py

from lark import Lark

# 1. Read the grammar file
with open("src/flow.lark", "r") as f:
    flow_grammar = f.read()

# 2. Create the Lark parser instance
#    start='start' tells Lark which rule to begin parsing with.
flow_parser = Lark(flow_grammar, start='start')

# 3. Read the code we want to parse
with open("examples/simple_copy.flow", "r") as f:
    flow_code = f.read()

# 4. Parse the code!
parse_tree = flow_parser.parse(flow_code)

# 5. Print the beautiful parse tree
print(parse_tree.pretty())