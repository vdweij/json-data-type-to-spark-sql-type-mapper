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

from json2spark_mapper.json_schema_drafts.drafts import JsonDraft, SupportedJsonDrafts
from json2spark_mapper.json_types.resolver import (
    AbstractResolver,
    AbstractStringResolver,
)


class TestAbstractResolvers(unittest.TestCase):
    def test_creation_of_abstract_class(self):
        with self.assertRaises(TypeError) as context:
            AbstractResolver()
        self.assertTrue(
            str(context.exception).startswith("Can't instantiate abstract class")
        )

    def test_creation_of_string_resolver(self):
        class TestStringResolver(AbstractStringResolver):
            def __init__(self, name: str, draft_support: set[JsonDraft]):
                super().__init__(name, draft_support)

            def resolve(self, json_snippet: str):
                return None

        # None name
        with self.assertRaises(ValueError) as context1:
            TestStringResolver(None, None)
        self.assertEqual(str(context1.exception), "A resolver requires a name (string)")

        # wrong type name
        with self.assertRaises(ValueError) as context1:
            TestStringResolver(True, None)
        self.assertEqual(str(context1.exception), "A resolver requires a name (string)")

        # empty type name
        with self.assertRaises(ValueError) as context1:
            TestStringResolver("", None)
        self.assertEqual(str(context1.exception), "A resolver requires a name (string)")

        # None set
        with self.assertRaises(ValueError) as context2:
            TestStringResolver("Test String Resolver", None)
        self.assertEqual(
            str(context2.exception),
            "The draft_support set should not be empty and should contain JsonDraft entries",
        )

        # empty set
        with self.assertRaises(ValueError) as context2:
            TestStringResolver("Test String Resolver", set())
        self.assertEqual(
            str(context2.exception),
            "The draft_support set should not be empty and should contain JsonDraft entries",
        )

        # wrong type set
        with self.assertRaises(ValueError) as context2:
            TestStringResolver(
                "Test String Resolver", {True, SupportedJsonDrafts.draft_0}
            )
        self.assertEqual(
            str(context2.exception),
            "The draft_support set should not be empty and should contain JsonDraft entries",
        )

        # valid
        TestStringResolver(
            "Test String Resolver",
            {SupportedJsonDrafts.draft_0, SupportedJsonDrafts.draft_1},
        )


if __name__ == "__main__":
    unittest.main()
