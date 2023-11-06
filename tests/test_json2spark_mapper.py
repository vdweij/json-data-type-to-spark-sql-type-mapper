import unittest
import json
from pyspark.sql.types import StructType, StructField, ArrayType, ByteType, ShortType, IntegerType, LongType, FloatType, DoubleType, StringType, BooleanType, TimestampType, NullType
import sys
sys.path.append('.')

from json2spark_mapper import json2spark_mapper

class TestMappings(unittest.TestCase):

    def test_no_input(self):
        print("starting test no input")
        schema = None
        with self.assertRaises(TypeError):
            # this should trow an error
            json2spark_mapper.map_json_schema_to_spark_schema(schema)       
       
    def test_no_json_input(self):
        print("starting test no json input")
        schema = ""
        with self.assertRaises(TypeError):
            # this should trow an error
            json2spark_mapper.map_json_schema_to_spark_schema(schema)
            
    def test_empty_json_input(self):
        print("starting test empty json input")
        schema = "{}"
        with self.assertRaises(TypeError):
            # this should trow an error
            json2spark_mapper.map_json_schema_to_spark_schema(schema)
        
    def test_empty_schema(self):
        with open("tests/empty-schema.json") as schema_file:
            schema = json.load(schema_file)
        struct_type = json2spark_mapper.map_json_schema_to_spark_schema(schema)
        # expects an StructType with an empty field array
        self.assertIsInstance(struct_type, StructType)
        self.assertTrue(len(struct_type.fields) == 0)

    def test_simple_schema(self):
        with open("tests/simple-schema.json") as schema_file:
            schema = json.load(schema_file)
        struct_type = json2spark_mapper.map_json_schema_to_spark_schema(schema)
        # expects an StructType with a field array of length 3
        self.assertIsInstance(struct_type, StructType)
        self.assertTrue(len(struct_type.fields) == 3)
        
    def test_int_type_schema(self):
        with open("tests/int-type-schema.json") as schema_file:
            schema = json.load(schema_file)
        struct_type = json2spark_mapper.map_json_schema_to_spark_schema(schema)
        # expects an StructType with a field array of length 5
        self.assertIsInstance(struct_type, StructType)
        self.assertTrue(len(struct_type.fields) == 5)
        self.assertEqual((struct_type.fields[struct_type.fieldNames().index("intValueNoRange")]).dataType, LongType())
        self.assertEqual((struct_type.fields[struct_type.fieldNames().index("intValueByteType")]).dataType, ByteType())
        self.assertEqual((struct_type.fields[struct_type.fieldNames().index("intValueShortType")]).dataType, ShortType())
        self.assertEqual((struct_type.fields[struct_type.fieldNames().index("intValueIntegerType")]).dataType, IntegerType())
        self.assertEqual((struct_type.fields[struct_type.fieldNames().index("intValueLongType")]).dataType, LongType())

if __name__ == '__main__':
    unittest.main()
