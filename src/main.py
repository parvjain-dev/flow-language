# src/main.py

from lark import Lark
from transpiler import FlowTranspiler
from validator import Validator

# --- Step 1: Parsing ---
with open("src/flow.lark", "r") as f:
    flow_grammar = f.read()

flow_parser = Lark(flow_grammar, start='start')

# Choose which file to run by uncommenting it
# test_file = "examples/mutate_test.flow"
# test_file = "examples/invalid_schema_test.flow"
# test_file = "examples/postgres_test.flow"
# test_file = "examples/invalid_mutate.flow" 
test_file = "examples/aggregate_test.flow"
with open(test_file, "r") as f:
    flow_code = f.read()

parse_tree = flow_parser.parse(flow_code)


# --- Step 2: Validation ---
# We run the transpiler once just to collect the schema info
schema_collector = FlowTranspiler()
schema_collector.transform(parse_tree)

# Now, we validate the tree using the collected schemas
validator = Validator(schema_collector.schemas, schema_collector.variable_schemas)
try:
    validator.visit(parse_tree)
    print("✅ Validation successful!")
except ValueError as e:
    print(f"❌ {e}")
    exit() # Stop if validation fails


# --- Step 3: Transpilation ---
# If validation passed, we run the transpiler again to generate the code
transpiler = FlowTranspiler()
python_script = transpiler.transform(parse_tree)

print("\n--- Generated Python Script ---")
print(python_script)