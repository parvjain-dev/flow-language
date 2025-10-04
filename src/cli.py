# src/cli.py

import click
from lark import Lark
from .transpiler import FlowTranspiler
from .validator import Validator

def run_flow_script(filepath: str):
    """
    The main function that orchestrates the entire Flow language process.
    """
    # --- Step 1: Parsing ---
    try:
        with open("src/flow.lark", "r") as f:
            flow_grammar = f.read()
        with open(filepath, "r") as f:
            flow_code = f.read()
    except FileNotFoundError as e:
        print(f"❌ Error: Could not find a required file. {e}")
        return

    flow_parser = Lark(flow_grammar, start='start')
    parse_tree = flow_parser.parse(flow_code)

    # --- Step 2: Validation ---
    try:
        schema_collector = FlowTranspiler()
        schema_collector.transform(parse_tree)
        validator = Validator(schema_collector.schemas, schema_collector.variable_schemas)
        validator.visit(parse_tree)
        print("✅ Validation successful!")
    except Exception as e:
        print(f"❌ {e}")
        return # Stop if validation fails

    # --- Step 3: Transpilation & Execution ---
    transpiler = FlowTranspiler()
    python_script = transpiler.transform(parse_tree)
    
    print("\n--- Generated Python Script ---")
    print(python_script)
    print("\n--- Running Script ---")
    try:
        # The exec() function executes the Python code stored in the string
        exec(python_script, globals())
        print("\n✅ Script finished successfully.")
    except Exception as e:
        print(f"\n❌ An error occurred during script execution: {e}")


# --- Click CLI Definition ---

@click.group()
def cli():
    """A command-line tool for the Flow language."""
    pass

@cli.command()
@click.argument('filepath', type=click.Path(exists=True))
def run(filepath):
    """Parses, validates, and executes a .flow script."""
    print(f"--- Running Flow script: {filepath} ---\n")
    run_flow_script(filepath)

if __name__ == '__main__':
    cli()