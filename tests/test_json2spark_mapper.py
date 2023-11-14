import unittest
import json
from pyspark.sql.types import StructType, StructField, ArrayType, ByteType, ShortType, IntegerType, LongType, FloatType, DoubleType, StringType, BooleanType, TimestampType, DateType, NullType
import sys
sys.path.append('.')

from json2spark_mapper.schema_mapper import from_json_to_spark

class TestMappings(unittest.TestCase):

    def test_no_input(self):
        print("starting test no input")
        schema = None
        with self.assertRaises(TypeError):
            # this should trow an error
            from_json_to_spark(schema)       
       
    def test_no_json_input(self):
        print("starting test no json input")
        schema = ""
        with self.assertRaises(TypeError):
            # this should trow an error
            from_json_to_spark(schema)
            
    def test_empty_json_input(self):
        print("starting test empty json input")
        schema = "{}"
        with self.assertRaises(TypeError):
            # this should trow an error
            from_json_to_spark(schema)
        
    def test_empty_schema(self):
        with open("tests/empty-schema.json") as schema_file:
            schema = json.load(schema_file)
        struct_type = from_json_to_spark(schema)
        # expects an StructType with an empty field array
        self.assertIsInstance(struct_type, StructType)
        self.assertTrue(len(struct_type.fields) == 0)

    def test_simple_schema(self):
        with open("tests/simple-schema.json") as schema_file:
            schema = json.load(schema_file)
        struct_type = from_json_to_spark(schema)
        # expects an StructType with a field array of length 3
        self.assertIsInstance(struct_type, StructType)
        self.assertTrue(len(struct_type.fields) == 3)
    
    def test_str_type_schema(self):
        with open("tests/string-type-schema.json") as schema_file:
            schema = json.load(schema_file)
        struct_type = from_json_to_spark(schema)
        # expects an StructType with a field array of length 1
        self.assertIsInstance(struct_type, StructType)
        self.assertTrue(len(struct_type.fields) == 1)
        stringValueType = struct_type.fields[struct_type.fieldNames().index("stringValue")]
        self.assertEqual(stringValueType.dataType, StringType())
        self.assertTrue(stringValueType.nullable)
        
    def test_str_datetime_type_schema(self):
        with open("tests/string-datetime-type-schema.json") as schema_file:
            schema = json.load(schema_file)
        struct_type = from_json_to_spark(schema)
        # expects an StructType with a field array of length 3
        self.assertIsInstance(struct_type, StructType)
        self.assertTrue(len(struct_type.fields) == 3)
        self.assertEqual((struct_type.fields[struct_type.fieldNames().index("stringDateTimeValue")]).dataType, TimestampType())
        self.assertEqual((struct_type.fields[struct_type.fieldNames().index("stringDateValue")]).dataType, DateType())  
        self.assertEqual((struct_type.fields[struct_type.fieldNames().index("stringTimeValue")]).dataType, StringType())          
        
    def test_bool_type_schema(self):
        with open("tests/bool-type-schema.json") as schema_file:
            schema = json.load(schema_file)
        struct_type = from_json_to_spark(schema)
        # expects an StructType with a field array of length 1
        self.assertIsInstance(struct_type, StructType)
        self.assertTrue(len(struct_type.fields) == 1)
        self.assertEqual((struct_type.fields[struct_type.fieldNames().index("boolValue")]).dataType, BooleanType())
        
    def test_int_type_schema(self):
        with open("tests/int-type-schema.json") as schema_file:
            schema = json.load(schema_file)
        struct_type = from_json_to_spark(schema)
        # expects an StructType with a field array of length 5
        self.assertIsInstance(struct_type, StructType)
        self.assertTrue(len(struct_type.fields) == 5)
        self.assertEqual((struct_type.fields[struct_type.fieldNames().index("intValueNoRange")]).dataType, LongType())
        self.assertEqual((struct_type.fields[struct_type.fieldNames().index("intValueByteType")]).dataType, ByteType())
        self.assertEqual((struct_type.fields[struct_type.fieldNames().index("intValueShortType")]).dataType, ShortType())
        self.assertEqual((struct_type.fields[struct_type.fieldNames().index("intValueIntegerType")]).dataType, IntegerType())
        self.assertEqual((struct_type.fields[struct_type.fieldNames().index("intValueLongType")]).dataType, LongType())

    def test_num_type_schema(self):
        with open("tests/num-type-schema.json") as schema_file:
            schema = json.load(schema_file)
        struct_type = from_json_to_spark(schema)
        # expects an StructType with a field array of length 2
        self.assertIsInstance(struct_type, StructType)
        self.assertTrue(len(struct_type.fields) == 2)
        self.assertEqual((struct_type.fields[struct_type.fieldNames().index("numValueNoRange")]).dataType, DoubleType())
        self.assertEqual((struct_type.fields[struct_type.fieldNames().index("numValueWithRange")]).dataType, DoubleType())
        
    def test_null_type_schema(self):
        with open("tests/null-type-schema.json") as schema_file:
            schema = json.load(schema_file)
        struct_type = from_json_to_spark(schema)
        # expects an StructType with a field array of length 1
        self.assertIsInstance(struct_type, StructType)
        self.assertTrue(len(struct_type.fields) == 1)
        self.assertEqual((struct_type.fields[struct_type.fieldNames().index("nullValue")]).dataType, NullType())

    def test_any_type_schema(self):
        with open("tests/any-type-schema.json") as schema_file:
            schema = json.load(schema_file)
        struct_type = from_json_to_spark(schema)
        # expects an StructType with a field array of length 1
        self.assertIsInstance(struct_type, StructType)
        self.assertTrue(len(struct_type.fields) == 1)
        self.assertEqual((struct_type.fields[struct_type.fieldNames().index("anyValue")]).dataType, StringType())

    def test_anyof_type_schema(self):
        with open("tests/anyof-type-schema.json") as schema_file:
            schema = json.load(schema_file)
        struct_type = from_json_to_spark(schema)
        # expects an StructType with a field array of length 1
        self.assertIsInstance(struct_type, StructType)
        self.assertTrue(len(struct_type.fields) == 1)
        self.assertEqual((struct_type.fields[struct_type.fieldNames().index("anyOfValue")]).dataType, StringType())
        
    def test_const_type_schema(self):
        with open("tests/const-type-schema.json") as schema_file:
            schema = json.load(schema_file)
        struct_type = from_json_to_spark(schema)
        # expects an StructType with a field array of length 1
        self.assertIsInstance(struct_type, StructType)
        self.assertTrue(len(struct_type.fields) == 1)
        self.assertEqual((struct_type.fields[struct_type.fieldNames().index("constValue")]).dataType, StringType())
        
    #######
    #
    # Complex type testing
    #
    #######
    
    # test simple object
    def test_simple_object_type_schema(self):
        with open("tests/simple-object-type-schema.json") as schema_file:
            schema = json.load(schema_file)
        struct_type = from_json_to_spark(schema)
        # expects an StructType with a field array of length 1
        self.assertIsInstance(struct_type, StructType)
        self.assertTrue(len(struct_type.fields) == 1)
        # compare type name, because instances will have different content
        simpleObject = struct_type.fields[struct_type.fieldNames().index("simpleObjectValue")]
        self.assertEqual(simpleObject.dataType.typeName, StructType().typeName)
        simpleObjectDataType = simpleObject.dataType
        # Check nested properties
        self.assertTrue(len(simpleObjectDataType.fields) == 3)
        self.assertEqual((simpleObjectDataType[simpleObjectDataType.fieldNames().index("stringValue")]).dataType, StringType())
        self.assertEqual((simpleObjectDataType[simpleObjectDataType.fieldNames().index("intValue")]).dataType, LongType())
        self.assertEqual((simpleObjectDataType[simpleObjectDataType.fieldNames().index("boolValue")]).dataType, BooleanType())
    
    
    # test complex object
    def test_complex_object_type_schema(self):
        with open("tests/complex-object-type-schema.json") as schema_file:
            schema = json.load(schema_file)
        struct_type = from_json_to_spark(schema)
        # expects an StructType with a field array of length 1
        self.assertIsInstance(struct_type, StructType)
        self.assertTrue(len(struct_type.fields) == 2)
        self.assertEqual((struct_type.fields[struct_type.fieldNames().index("stringValue")]).dataType, StringType())
        # compare type name, because instances will have different content
        complexObject = struct_type.fields[struct_type.fieldNames().index("complexObjectValue")]
        self.assertEqual(complexObject.dataType.typeName, StructType().typeName)
        simpleObjectDataType = complexObject.dataType
        # Check nested properties
        self.assertTrue(len(simpleObjectDataType.fields) == 3)
        self.assertEqual((simpleObjectDataType[simpleObjectDataType.fieldNames().index("stringValue")]).dataType, StringType())
        self.assertEqual((simpleObjectDataType[simpleObjectDataType.fieldNames().index("arrayValue")]).dataType, ArrayType(StringType()))
        # compare type name, because instances will have different content
        self.assertEqual((simpleObjectDataType[simpleObjectDataType.fieldNames().index("nestedObjectValue")]).dataType.typeName, StructType.typeName)
    
    # test simple array
    def test_simple_array_type_schema(self):
        with open("tests/simple-array-type-schema.json") as schema_file:
            schema = json.load(schema_file)
        struct_type = from_json_to_spark(schema)
        # expects an StructType with a field array of length 1
        self.assertIsInstance(struct_type, StructType)
        self.assertTrue(len(struct_type.fields) == 1)
        # compare type name, because instances will have different content
        simpleArray = struct_type.fields[struct_type.fieldNames().index("simpleArrayValue")]
        self.assertEqual(simpleArray.dataType.typeName, ArrayType(elementType=StringType()).typeName) # ArrayType needs an init paramter
        # Check elementType properties
        self.assertEqual(simpleArray.dataType.elementType, DoubleType())
    
    # test complex array
    def test_complex_array_type_schema(self):
        with open("tests/complex-array-type-schema.json") as schema_file:
            schema = json.load(schema_file)
        struct_type = from_json_to_spark(schema)
        # expects an StructType with a field array of length 1
        self.assertIsInstance(struct_type, StructType)
        self.assertTrue(len(struct_type.fields) == 1)
        # compare type name, because instances will have different content
        complexArray = struct_type.fields[struct_type.fieldNames().index("complexArrayValue")]
        self.assertEqual(complexArray.dataType.typeName, ArrayType(elementType=StringType()).typeName) # ArrayType needs an init paramter
        # Check elementType properties
        # TODO: fix test
        # self.assertEqual(complexArray.dataType.elementType, DoubleType())
        
    # test required fields
    def test_required_simple_fields(self):
        with open("tests/required-simple-fields-schema.json") as schema_file:
            schema = json.load(schema_file)
        struct_type = from_json_to_spark(schema)
        # expects an StructType with a field array of length 4
        self.assertIsInstance(struct_type, StructType)
        self.assertTrue(len(struct_type.fields) == 4)
        
        stringValueType = struct_type.fields[struct_type.fieldNames().index("stringValue")]
        self.assertEqual(stringValueType.dataType, StringType())
        self.assertFalse(stringValueType.nullable)
        
        numberValueType = struct_type.fields[struct_type.fieldNames().index("numberValue")]
        self.assertEqual(numberValueType.dataType, DoubleType())
        self.assertFalse(numberValueType.nullable)
        
        integerValueType = struct_type.fields[struct_type.fieldNames().index("integerValue")]
        self.assertEqual(integerValueType.dataType, LongType())
        self.assertFalse(integerValueType.nullable)
        
        booleanValueType = struct_type.fields[struct_type.fieldNames().index("booleanValue")]
        self.assertEqual(booleanValueType.dataType, BooleanType())
        self.assertFalse(booleanValueType.nullable)
    
    # test required fields
    def test_required_complex_fields(self):
        with open("tests/required-complex-fields-schema.json") as schema_file:
            schema = json.load(schema_file)
        struct_type = from_json_to_spark(schema)
        # expects an StructType with a field array of length 2
        self.assertIsInstance(struct_type, StructType)
        self.assertTrue(len(struct_type.fields) == 2)
        
        # TODO: complete
        
if __name__ == '__main__':
    unittest.main()
