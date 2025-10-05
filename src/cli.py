# src/cli.py

import click
from lark import Lark
from .transpiler import FlowTranspiler
from .validator import Validator
from .runner import TestRunner # <-- NEW IMPORT

# --- Main Logic Functions ---

def run_flow_script(filepath: str):
    # ... (this function is unchanged) ...
    try:
        with open("src/flow.lark", "r") as f: flow_grammar = f.read()
        with open(filepath, "r") as f: flow_code = f.read()
    except FileNotFoundError as e:
        print(f"❌ Error: Could not find a required file. {e}"); return
    flow_parser = Lark(flow_grammar, start='start')
    parse_tree = flow_parser.parse(flow_code)
    try:
        schema_collector = FlowTranspiler()
        schema_collector.transform(parse_tree)
        validator = Validator(schema_collector.schemas, schema_collector.variable_schemas)
        validator.visit(parse_tree)
        print("✅ Validation successful!")
    except Exception as e:
        print(f"❌ {e}"); return
    transpiler = FlowTranspiler()
    python_script = transpiler.transform(parse_tree)
    print("\n--- Generated Python Script ---")
    print(python_script)
    print("\n--- Running Script ---")
    try:
        exec(python_script, globals())
        print("\n✅ Script finished successfully.")
    except Exception as e:
        print(f"\n❌ An error occurred during script execution: {e}")

def run_flow_tests(filepath: str):
    """
    Parses a .flow file and runs any test blocks found within it.
    """
    try:
        with open("src/flow.lark", "r") as f: flow_grammar = f.read()
        with open(filepath, "r") as f: flow_code = f.read()
    except FileNotFoundError as e:
        print(f"❌ Error: Could not find a required file. {e}"); return
        
    flow_parser = Lark(flow_grammar, start='start')
    parse_tree = flow_parser.parse(flow_code)

    runner = TestRunner(parse_tree)
    runner.run()

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

# NEW: The 'test' command
@cli.command()
@click.argument('filepath', type=click.Path(exists=True))
def test(filepath):
    """Finds and runs tests in a .flow file."""
    print(f"--- Running tests in: {filepath} ---\n")
    run_flow_tests(filepath)

if __name__ == '__main__':
    cli()