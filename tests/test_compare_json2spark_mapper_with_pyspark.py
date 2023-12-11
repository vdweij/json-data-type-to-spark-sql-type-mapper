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
import json
import logging
import unittest

import jsonschema
from pyspark.sql import SparkSession

from json2spark_mapper.schema_mapper import from_json_to_spark


class TestMappings(unittest.TestCase):
    spark = None

    @classmethod
    def setUpClass(self):
        print("Setting up resources for the test class.")
        print("Creating Spark session.")
        self.spark = SparkSession.builder.appName(
            "UNITTEST_JSON2SPARK_MAPPER"
        ).getOrCreate()

        print("Setting up module logger")
        module_logger = logging.getLogger("json2spark_mapper")
        module_logger.setLevel(logging.DEBUG)
        module_logger.addHandler(logging.StreamHandler())

    @classmethod
    def tearDownClass(self):
        print("Cleaning up resources after the test class.")
        print("Stopping Spark session.")
        self.spark.stop()

    # test complex array
    def test_complex_array_type_schema(self):
        struct_type, inferred_schema = self._fetch_struct_types(
            "tests/schemas/arrays/complex-array-type-schema.json",
            "tests/documents/arrays/complex-array-type.json",
        )

        self.assertEqual(struct_type, inferred_schema)

    # test complex array with objects
    def test_array_contains_objects_schema(self):
        struct_type, inferred_schema = self._fetch_struct_types(
            "tests/schemas/arrays/array-contains-objects-types-schema.json",
            "tests/documents/arrays/array-contains-objects.json",
        )

        # TODO: there is a difference between pyspark inferrence
        # self.assertEqual(struct_type, inferred_schema)

    def _fetch_struct_types(self, json_schema, json_document):
        with open(json_schema) as schema_file:
            schema = json.load(schema_file)

        with open(json_document) as document_file:
            document = json.load(document_file)

        # first check is json document is valid
        jsonschema.validate(instance=document, schema=schema)

        # get struct types from schema
        struct_type = from_json_to_spark(schema)

        print("Created struct type from json schema:")
        print(struct_type)

        # get struct type from inferrence
        inferred_schema = self._infer_schema(json_document)

        print("Inferred struct type from json document:")
        print(inferred_schema)

        return struct_type, inferred_schema

    def _infer_schema(self, json_document):
        df = (
            self.spark.read.option("multiline", "true")
            .option("mode", "PERMISSIVE")
            .json(json_document)
        )

        return df.schema


if __name__ == "__main__":
    unittest.main()
