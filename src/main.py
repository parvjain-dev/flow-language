# src/main.py

from lark import Lark
from .transpiler import FlowTranspiler
from .validator import Validator

def run_flow_script(filepath: str):
    """
    Parses, validates, transpiles, and executes a .flow script.
    """
    # --- Step 1: Parsing ---
    with open("src/flow.lark", "r") as f:
        flow_grammar = f.read()

    flow_parser = Lark(flow_grammar, start='start')

    try:
        with open(filepath, "r") as f:
            flow_code = f.read()
    except FileNotFoundError:
        print(f"❌ Error: File not found at '{filepath}'")
        return

    parse_tree = flow_parser.parse(flow_code)

    # --- Step 2: Validation ---
    schema_collector = FlowTranspiler()
    schema_collector.transform(parse_tree)

    validator = Validator(schema_collector.schemas, schema_collector.variable_schemas)
    try:
        validator.visit(parse_tree)
        print("✅ Validation successful!")
    except ValueError as e:
        print(f"❌ {e}")
        return # Stop if validation fails

    # --- Step 3: Transpilation ---
    transpiler = FlowTranspiler()
    python_script = transpiler.transform(parse_tree)
    
    print("\n--- Generated Python Script ---")
    print(python_script)

    # --- Step 4: Execution ---
    print("\n--- Running Script ---")
    try:
        # The exec() function executes the Python code stored in the string
        exec(python_script, globals())
        print("\n✅ Script finished successfully.")
    except Exception as e:
        print(f"\n❌ An error occurred during script execution: {e}")