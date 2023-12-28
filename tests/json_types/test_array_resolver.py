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

from pyspark.sql.types import (
    ArrayType,
    DataType,
    DoubleType,
    NullType,
    StringType,
    StructType,
)

from json2spark_mapper.json_types.array_resolver import (
    Draft202012ArrayResolver,
)
from json2spark_mapper.json_types.resolver import (
    PropertyResolver,
)


class TestDraft202012ArrayResolver(unittest.TestCase):
    def test_list(self):
        json_snippet = {
            "type": "array",
            "items": {"type": "number"},
        }  # example taken from https://json-schema.org/understanding-json-schema/reference/array#items

        struc_field = Draft202012ArrayResolver().resolve(
            json_snippet, self.PROPERTY_RESOLVER
        )
        self.assertEqual(struc_field.typeName(), ArrayType(DoubleType()).typeName())

    def test_tuple_draft2012_no_additional_items(self):
        json_snippet = {
            "type": "array",
            "prefixItems": [
                {"type": "number"},
                {"type": "string"},
                {"enum": ["Street", "Avenue", "Boulevard"]},
                {"enum": ["NW", "NE", "SW", "SE"]},
            ],
        }  # example taken from https://json-schema.org/understanding-json-schema/reference/array#tupleValidation

        struc_field = Draft202012ArrayResolver().resolve(
            json_snippet, self.PROPERTY_RESOLVER
        )
        self.assertEqual(struc_field.typeName(), StructType().typeName())
        self.assertEqual(len(struc_field.fields), len(json_snippet["prefixItems"]))

    def test_tuple_draft2012_additional_items_false(self):
        json_snippet = {
            "type": "array",
            "prefixItems": [
                {"type": "number"},
                {"type": "string"},
                {"enum": ["Street", "Avenue", "Boulevard"]},
                {"enum": ["NW", "NE", "SW", "SE"]},
            ],
            "items": False,
        }  # example taken from https://json-schema.org/understanding-json-schema/reference/array#tupleValidation

        struc_field = Draft202012ArrayResolver().resolve(
            json_snippet, self.PROPERTY_RESOLVER
        )
        self.assertEqual(struc_field.typeName(), StructType().typeName())
        self.assertEqual(len(struc_field.fields), len(json_snippet["prefixItems"]))

    def test_tuple_draft2012_additional_items_true(self):
        json_snippet = {
            "type": "array",
            "prefixItems": [
                {"type": "number"},
                {"type": "string"},
                {"enum": ["Street", "Avenue", "Boulevard"]},
                {"enum": ["NW", "NE", "SW", "SE"]},
            ],
            "items": True,
        }  # example taken from https://json-schema.org/understanding-json-schema/reference/array#tupleValidation

        struc_field = Draft202012ArrayResolver().resolve(
            json_snippet, self.PROPERTY_RESOLVER
        )
        self.assertEqual(struc_field, ArrayType(StringType()))

    def test_contains(self):
        json_snippet = {
            "type": "array",
            "contains": {"type": "number"},
        }  # example taken from https://json-schema.org/understanding-json-schema/reference/array#contains

        struc_field = Draft202012ArrayResolver().resolve(
            json_snippet, self.PROPERTY_RESOLVER
        )
        self.assertEqual(struc_field.typeName(), ArrayType(DoubleType()).typeName())

    class DummyPropertyResolver(PropertyResolver):
        def resolve_properties(self, json_snippet: dict) -> StructType:
            # just to satisfy return type
            return NullType()

        def resolve_property_type(self, json_snippet: dict) -> DataType:
            # just to satisfy return type
            return NullType()

    PROPERTY_RESOLVER = DummyPropertyResolver()


if __name__ == "__main__":
    unittest.main()
