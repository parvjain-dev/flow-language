# src/runner.py

from lark import Tree
from .transpiler import FlowTranspiler

class TestRunner:
    def __init__(self, parse_tree):
        self.parse_tree = parse_tree
        self.test_blocks = []

    def find_test_blocks(self):
        """Finds all `test_block` nodes in the parse tree."""
        for node in self.parse_tree.children:
            if isinstance(node, Tree) and node.data == 'test_block':
                test_name = node.children[0].value[1:-1]
                # The actual statements are the rest of the children
                statements_tree = Tree('start', node.children[1:])
                self.test_blocks.append({'name': test_name, 'tree': statements_tree})

    def run(self):
        """Runs all the tests that were found."""
        self.find_test_blocks()
        print(f"Found {len(self.test_blocks)} test(s).\n")
        
        passed_count = 0
        failed_count = 0

        for test in self.test_blocks:
            print(f"▶️  Running test: '{test['name']}'")
            try:
                # Each test gets a fresh, isolated transpiler
                transpiler = FlowTranspiler()
                python_script = transpiler.transform(test['tree'])

                # The generated assert statements are handled by python's exec
                exec(python_script, globals())

                print(f"✅ PASSED: '{test['name']}'\n")
                passed_count += 1
            except AssertionError as e:
                print(f"❌ FAILED: '{test['name']}'")
                print(f"   Reason: Assertion failed. {e}\n")
                failed_count += 1
            except Exception as e:
                print(f"❌ ERROR: '{test['name']}'")
                print(f"   Reason: An unexpected error occurred. {e}\n")
                failed_count += 1

        print("-" * 20)
        print(f"Test Summary: {passed_count} passed, {failed_count} failed.")
        return failed_count == 0