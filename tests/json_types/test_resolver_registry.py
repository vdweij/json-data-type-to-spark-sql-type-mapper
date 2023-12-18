"""
# For debuging in VSCode add this launch file/snippet in .vscode/launch.json
{
    "version": "0.2.0",
    "configurations": [
        {
            "name": "Python: Unittest",
            "type": "python",
            "request": "launch",
            "program": "${file}",
            "purpose": ["debug-test"],
            "console": "integratedTerminal",
            "env": {"PYTHONPATH": "${workspaceFolder}${pathSeparator}${env:PYTHONPATH}"}
        }
    ]
}

# No longer needed:
# import sys
# sys.path.append(".")
"""
import unittest

from json2spark_mapper.json_schema_drafts.drafts import JSON_DRAFTS
from json2spark_mapper.json_types.resolver import (
    AbstractArrayResolver,
    AbstractBooleanResolver,
    AbstractIntegerResolver,
    AbstractNumberResolver,
    AbstractObjectResolver,
    AbstractStringResolver,
)
from json2spark_mapper.json_types.resolver_registry import ResolverRegistry


class TestResolverRegistry(unittest.TestCase):
    def test_creation_of_registry_invalid_draft(self):
        expected_msg = "A valid json draft should be specified"
        # None name
        with self.assertRaises(ValueError) as context:
            ResolverRegistry(None, None)
        self.assertEqual(str(context.exception), expected_msg)

        with self.assertRaises(ValueError) as context:
            ResolverRegistry("Invalid Type", None)
        self.assertEqual(str(context.exception), expected_msg)

    def test_creation_of_registry_invalid_resolvers(self):
        expected_msg = (
            "The resolvers set should not be empty and should contain Resolver entries"
        )
        # None name
        with self.assertRaises(ValueError) as context:
            ResolverRegistry(JSON_DRAFTS.draft_0, None)
        self.assertEqual(str(context.exception), expected_msg)

        with self.assertRaises(ValueError) as context:
            ResolverRegistry(JSON_DRAFTS.draft_0, "Invalid Type")
        self.assertEqual(str(context.exception), expected_msg)
        with self.assertRaises(ValueError) as context:
            ResolverRegistry(JSON_DRAFTS.draft_0, {"Invalid Type"})
        self.assertEqual(str(context.exception), expected_msg)

    def test_creation_of_registry_incomplete_resolvers(self):
        expected_msg_start = "No support found for json type"

        with self.assertRaises(ValueError) as context:
            ResolverRegistry(
                JSON_DRAFTS.draft_0, {TestResolverRegistry.TestStringResolver()}
            )
        self.assertTrue(str(context.exception).startswith(expected_msg_start))

        # wrong draft
        with self.assertRaises(ValueError) as context:
            ResolverRegistry(
                JSON_DRAFTS.draft_1, TestResolverRegistry.DRAFT_0_RESOLVERS
            )
        self.assertTrue(str(context.exception).startswith(expected_msg_start))

    def test_creation_of_registry(self):
        ResolverRegistry(JSON_DRAFTS.draft_0, TestResolverRegistry.DRAFT_0_RESOLVERS)

    class TestStringResolver(AbstractStringResolver):
        def __init__(self):
            super().__init__("TestStringResolver", {JSON_DRAFTS.draft_0})

        def resolve(self, json_snippet: dict):
            return None

    class TestIntegerResolver(AbstractIntegerResolver):
        def __init__(self):
            super().__init__("TestIntegerResolver", {JSON_DRAFTS.draft_0})

        def resolve(self, json_snippet: dict):
            return None

    class TestNumberResolver(AbstractNumberResolver):
        def __init__(self):
            super().__init__("TestNumberResolver", {JSON_DRAFTS.draft_0})

        def resolve(self, json_snippet: dict):
            return None

    class TestBooleanResolver(AbstractBooleanResolver):
        def __init__(self):
            super().__init__("TestBooleanResolver", {JSON_DRAFTS.draft_0})

        def resolve(self, json_snippet: dict):
            return None

    class TestArrayResolver(AbstractArrayResolver):
        def __init__(self):
            super().__init__("TestArrayResolver", {JSON_DRAFTS.draft_0})

        def resolve(self, json_snippet: dict):
            return None

    class TestObjectResolver(AbstractObjectResolver):
        def __init__(self):
            super().__init__("TestObjectResolver", {JSON_DRAFTS.draft_0})

        def resolve(self, json_snippet: dict):
            return None

    DRAFT_0_RESOLVERS = {
        TestStringResolver(),
        TestIntegerResolver(),
        TestNumberResolver(),
        TestBooleanResolver(),
        TestArrayResolver(),
        TestObjectResolver(),
    }


if __name__ == "__main__":
    unittest.main()
