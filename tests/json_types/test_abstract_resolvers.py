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
    AbstractStringResolver,
    JsonType,
    PropertyResolver,
    TypeResolver,
)


class TestAbstractResolvers(unittest.TestCase):
    def test_creation_of_abstract_class(self):
        with self.assertRaises(TypeError) as context:
            TypeResolver()
        self.assertTrue(
            str(context.exception).startswith("Can't instantiate abstract class")
        )

    def test_creation_of_string_resolver_invalid_name_param(self):
        # None name
        with self.assertRaises(ValueError) as context1:
            TestAbstractResolvers.TestStringResolver(None, None)
        self.assertEqual(
            str(context1.exception), "A type resolver requires a name (string)"
        )

        # wrong type name
        with self.assertRaises(ValueError) as context1:
            TestAbstractResolvers.TestStringResolver(True, None)
        self.assertEqual(
            str(context1.exception), "A type resolver requires a name (string)"
        )

        # empty type name
        with self.assertRaises(ValueError) as context1:
            TestAbstractResolvers.TestStringResolver("", None)
        self.assertEqual(
            str(context1.exception), "A type resolver requires a name (string)"
        )

    def test_creation_of_string_resolver_invalid_draft_param(self):
        # None set
        with self.assertRaises(ValueError) as context2:
            TestAbstractResolvers.TestStringResolver("Test String Resolver", None)
        self.assertEqual(
            str(context2.exception),
            "The draft_support set should not be empty and should contain JsonDraft entries",
        )

        # empty set
        with self.assertRaises(ValueError) as context2:
            TestAbstractResolvers.TestStringResolver("Test String Resolver", set())
        self.assertEqual(
            str(context2.exception),
            "The draft_support set should not be empty and should contain JsonDraft entries",
        )

        # wrong type set
        with self.assertRaises(ValueError) as context2:
            TestAbstractResolvers.TestStringResolver(
                "Test String Resolver", {True, SupportedJsonDrafts.draft_0}
            )
        self.assertEqual(
            str(context2.exception),
            "The draft_support set should not be empty and should contain JsonDraft entries",
        )

    def test_creation_of_valid_string_resolver(self):
        # valid
        string_resolver = TestAbstractResolvers.TestStringResolver(
            "Test String Resolver",
            {SupportedJsonDrafts.draft_0, SupportedJsonDrafts.draft_1},
        )

        # check supporting type
        self.assertTrue(string_resolver.supports_json_type(JsonType.STRING))
        self.assertFalse(string_resolver.supports_json_type(JsonType.INTEGER))
        self.assertFalse(string_resolver.supports_json_type(JsonType.NUMBER))
        self.assertFalse(string_resolver.supports_json_type(JsonType.BOOLEAN))
        self.assertFalse(string_resolver.supports_json_type(JsonType.ARRAY))
        self.assertFalse(string_resolver.supports_json_type(JsonType.OBJECT))
        self.assertFalse(string_resolver.supports_json_type(None))

        # check supporting draft
        self.assertTrue(
            string_resolver.supports_json_draft(SupportedJsonDrafts.draft_0)
        )
        self.assertTrue(
            string_resolver.supports_json_draft(SupportedJsonDrafts.draft_1)
        )
        self.assertFalse(
            string_resolver.supports_json_draft(SupportedJsonDrafts.draft_2)
        )
        self.assertFalse(
            string_resolver.supports_json_draft(SupportedJsonDrafts.draft_3)
        )
        self.assertFalse(
            string_resolver.supports_json_draft(SupportedJsonDrafts.draft_4)
        )
        self.assertFalse(
            string_resolver.supports_json_draft(SupportedJsonDrafts.draft_5)
        )
        self.assertFalse(
            string_resolver.supports_json_draft(SupportedJsonDrafts.draft_6)
        )
        self.assertFalse(
            string_resolver.supports_json_draft(SupportedJsonDrafts.draft_7)
        )
        self.assertFalse(
            string_resolver.supports_json_draft(SupportedJsonDrafts.draft_2019_09)
        )
        self.assertFalse(
            string_resolver.supports_json_draft(SupportedJsonDrafts.draft_2020_12)
        )

        # check covienence supports function
        self.assertTrue(
            string_resolver.supports(JsonType.STRING, SupportedJsonDrafts.draft_0)
        )
        self.assertTrue(
            string_resolver.supports(JsonType.STRING, SupportedJsonDrafts.draft_1)
        )
        self.assertFalse(
            string_resolver.supports(JsonType.BOOLEAN, SupportedJsonDrafts.draft_0)
        )
        self.assertFalse(
            string_resolver.supports(JsonType.BOOLEAN, SupportedJsonDrafts.draft_1)
        )
        self.assertFalse(
            string_resolver.supports(JsonType.STRING, SupportedJsonDrafts.draft_2)
        )
        self.assertFalse(
            string_resolver.supports(JsonType.STRING, SupportedJsonDrafts.draft_3)
        )
        # believe the other permutations...

        self.assertFalse(string_resolver.supports_json_draft(None))

    class TestStringResolver(AbstractStringResolver):
        def __init__(self, name: str, draft_support: set[JsonDraft]):
            super().__init__(name, draft_support)

        def resolve(
            self,
            json_snippet: dict,
            property_resolver: PropertyResolver | None,
        ):
            return None


if __name__ == "__main__":
    unittest.main()
