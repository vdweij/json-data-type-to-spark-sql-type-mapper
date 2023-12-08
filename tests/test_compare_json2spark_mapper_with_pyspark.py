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
        with open("tests/schemas/arrays/complex-array-type-schema.json") as schema_file:
            schema = json.load(schema_file)
        struct_type = from_json_to_spark(schema)

        print("Created struct type from json schema:")
        print(struct_type)

        # use a sample document that matches the schema
        inferred_schema = self._infer_schema(
            "tests/documents/arrays/complex-array-type.json"
        )

        print("Inferred struct type from json document:")
        print(inferred_schema)

        self.assertEqual(struct_type, inferred_schema)

    # test complex array

    def test_array_contains_objects_schema(self):
        logging.getLogger("json2spark_mapper").setLevel(logging.DEBUG)
        with open(
            "tests/schemas/arrays/array-contains-objects-types-schema.json"
        ) as schema_file:
            schema = json.load(schema_file)
        struct_type = from_json_to_spark(schema)

        print("Created struct type from json schema:")
        print(struct_type)

        # use a sample document that matches the schema
        inferred_schema = self._infer_schema(
            "tests/documents/arrays/array-contains-objects.json"
        )

        print("Inferred struct type from json document:")
        print(inferred_schema)

        # TODO: there is a difference between pyspark inferrence
        # self.assertEqual(struct_type, inferred_schema)

    def _infer_schema(self, json_document):
        df = (
            self.spark.read.option("multiline", "true")
            .option("mode", "PERMISSIVE")
            .json(json_document)
        )

        return df.schema


if __name__ == "__main__":
    unittest.main()
