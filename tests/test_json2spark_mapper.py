import json
#import sys
import unittest

from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    ByteType,
    DateType,
    DoubleType,
    IntegerType,
    LongType,
    NullType,
    ShortType,
    StringType,
    StructType,
    TimestampType,
)

#sys.path.append(".")  # needed for debugging in VSCode
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
        with open("tests/schemas/empty-schema.json") as schema_file:
            schema = json.load(schema_file)
        struct_type = from_json_to_spark(schema)
        # expects an StructType with an empty field array
        self.assertIsInstance(struct_type, StructType)
        self.assertTrue(len(struct_type.fields) == 0)

    def test_simple_schema(self):
        with open("tests/schemas/simple-schema.json") as schema_file:
            schema = json.load(schema_file)
        struct_type = from_json_to_spark(schema)
        # expects an StructType with a field array of length 3
        expected_length = 3
        self.assertIsInstance(struct_type, StructType)
        self.assertTrue(len(struct_type.fields) == expected_length)

    def test_str_type_schema(self):
        with open("tests/schemas/string-type-schema.json") as schema_file:
            schema = json.load(schema_file)
        struct_type = from_json_to_spark(schema)
        # expects an StructType with a field array of length 1
        self.assertIsInstance(struct_type, StructType)
        self.assertTrue(len(struct_type.fields) == 1)
        stringValueType = struct_type.fields[
            struct_type.fieldNames().index("stringValue")
        ]
        self.assertEqual(stringValueType.dataType, StringType())
        self.assertTrue(stringValueType.nullable)

    def test_str_datetime_type_schema(self):
        with open("tests/schemas/string-datetime-type-schema.json") as schema_file:
            schema = json.load(schema_file)
        struct_type = from_json_to_spark(schema)
        # expects an StructType with a field array of length 3
        expected_length = 3
        self.assertIsInstance(struct_type, StructType)
        self.assertTrue(len(struct_type.fields) == expected_length)
        self.assertEqual(
            (
                struct_type.fields[
                    struct_type.fieldNames().index("stringDateTimeValue")
                ]
            ).dataType,
            TimestampType(),
        )
        self.assertEqual(
            (
                struct_type.fields[struct_type.fieldNames().index("stringDateValue")]
            ).dataType,
            DateType(),
        )
        self.assertEqual(
            (
                struct_type.fields[struct_type.fieldNames().index("stringTimeValue")]
            ).dataType,
            StringType(),
        )

    def test_bool_type_schema(self):
        with open("tests/schemas/bool-type-schema.json") as schema_file:
            schema = json.load(schema_file)
        struct_type = from_json_to_spark(schema)
        # expects an StructType with a field array of length 1
        self.assertIsInstance(struct_type, StructType)
        self.assertTrue(len(struct_type.fields) == 1)
        self.assertEqual(
            (struct_type.fields[struct_type.fieldNames().index("boolValue")]).dataType,
            BooleanType(),
        )

    def test_int_type_schema(self):
        with open("tests/schemas/int-type-schema.json") as schema_file:
            schema = json.load(schema_file)
        struct_type = from_json_to_spark(schema)
        # expects an StructType with a field array of length 5
        expected_length = 5
        self.assertIsInstance(struct_type, StructType)
        self.assertTrue(len(struct_type.fields) == expected_length)
        self.assertEqual(
            (
                struct_type.fields[struct_type.fieldNames().index("intValueNoRange")]
            ).dataType,
            LongType(),
        )
        self.assertEqual(
            (
                struct_type.fields[struct_type.fieldNames().index("intValueByteType")]
            ).dataType,
            ByteType(),
        )
        self.assertEqual(
            (
                struct_type.fields[struct_type.fieldNames().index("intValueShortType")]
            ).dataType,
            ShortType(),
        )
        self.assertEqual(
            (
                struct_type.fields[
                    struct_type.fieldNames().index("intValueIntegerType")
                ]
            ).dataType,
            IntegerType(),
        )
        self.assertEqual(
            (
                struct_type.fields[struct_type.fieldNames().index("intValueLongType")]
            ).dataType,
            LongType(),
        )

    def test_num_type_schema(self):
        with open("tests/schemas/num-type-schema.json") as schema_file:
            schema = json.load(schema_file)
        struct_type = from_json_to_spark(schema)
        # expects an StructType with a field array of length 2
        expected_length = 2
        self.assertIsInstance(struct_type, StructType)
        self.assertTrue(len(struct_type.fields) == expected_length)
        self.assertEqual(
            (
                struct_type.fields[struct_type.fieldNames().index("numValueNoRange")]
            ).dataType,
            DoubleType(),
        )
        self.assertEqual(
            (
                struct_type.fields[struct_type.fieldNames().index("numValueWithRange")]
            ).dataType,
            DoubleType(),
        )

    def test_null_type_schema(self):
        with open("tests/schemas/null-type-schema.json") as schema_file:
            schema = json.load(schema_file)
        struct_type = from_json_to_spark(schema)
        # expects an StructType with a field array of length 1
        self.assertIsInstance(struct_type, StructType)
        self.assertTrue(len(struct_type.fields) == 1)
        self.assertEqual(
            (struct_type.fields[struct_type.fieldNames().index("nullValue")]).dataType,
            NullType(),
        )

    def test_any_type_schema(self):
        with open("tests/schemas/any-type-schema.json") as schema_file:
            schema = json.load(schema_file)
        struct_type = from_json_to_spark(schema)
        # expects an StructType with a field array of length 1
        self.assertIsInstance(struct_type, StructType)
        self.assertTrue(len(struct_type.fields) == 1)
        self.assertEqual(
            (struct_type.fields[struct_type.fieldNames().index("anyValue")]).dataType,
            StringType(),
        )

    def test_anyof_type_schema(self):
        with open("tests/schemas/anyof-type-schema.json") as schema_file:
            schema = json.load(schema_file)
        struct_type = from_json_to_spark(schema)
        # expects an StructType with a field array of length 1
        self.assertIsInstance(struct_type, StructType)
        self.assertTrue(len(struct_type.fields) == 1)
        self.assertEqual(
            (struct_type.fields[struct_type.fieldNames().index("anyOfValue")]).dataType,
            StringType(),
        )

    def test_const_type_schema(self):
        with open("tests/schemas/const-type-schema.json") as schema_file:
            schema = json.load(schema_file)
        struct_type = from_json_to_spark(schema)
        # expects an StructType with a field array of length 1
        self.assertIsInstance(struct_type, StructType)
        self.assertTrue(len(struct_type.fields) == 1)
        self.assertEqual(
            (struct_type.fields[struct_type.fieldNames().index("constValue")]).dataType,
            StringType(),
        )

    def test_multi_type_schema(self):
        with open("tests/schemas/multi-type-schema.json") as schema_file:
            schema = json.load(schema_file)
        struct_type = from_json_to_spark(schema)
        # expects an StructType with a field array of length 1
        self.assertIsInstance(struct_type, StructType)
        self.assertTrue(len(struct_type.fields) == 1)
        self.assertEqual(
            (
                struct_type.fields[struct_type.fieldNames().index("multiTypeValue")]
            ).dataType,
            StringType(),
        )

    def test_optional_null_type_schema(self):
        with open("tests/schemas/optional-null-type-schema.json") as schema_file:
            schema = json.load(schema_file)
        struct_type = from_json_to_spark(schema)
        # expects an StructType with a field array of length 1
        self.assertIsInstance(struct_type, StructType)
        self.assertTrue(len(struct_type.fields) == 1)
        self.assertEqual(
            (
                struct_type.fields[
                    struct_type.fieldNames().index("optionalDateTimeValue")
                ]
            ).dataType,
            TimestampType(),
        )

    #######
    #
    # Complex type testing
    #
    #######

    # test simple object
    def test_simple_object_type_schema(self):
        with open(
            "tests/schemas/objects/simple-object-type-schema.json"
        ) as schema_file:
            schema = json.load(schema_file)
        struct_type = from_json_to_spark(schema)
        # expects an StructType with a field array of length 1
        self.assertIsInstance(struct_type, StructType)
        self.assertTrue(len(struct_type.fields) == 1)
        # compare type name, because instances will have different content
        simple_object = struct_type.fields[
            struct_type.fieldNames().index("simpleObjectValue")
        ]
        self.assertEqual(simple_object.dataType.typeName, StructType().typeName)
        self.assertTrue(simple_object.nullable)

        simple_object_data_type = simple_object.dataType
        # Check nested properties
        expected_length = 3
        self.assertTrue(len(simple_object_data_type.fields) == expected_length)
        self.assertEqual(
            (
                simple_object_data_type[
                    simple_object_data_type.fieldNames().index("stringValue")
                ]
            ).dataType,
            StringType(),
        )
        self.assertEqual(
            (
                simple_object_data_type[
                    simple_object_data_type.fieldNames().index("intValue")
                ]
            ).dataType,
            LongType(),
        )
        self.assertEqual(
            (
                simple_object_data_type[
                    simple_object_data_type.fieldNames().index("boolValue")
                ]
            ).dataType,
            BooleanType(),
        )

    # test complex object
    def test_complex_object_type_schema(self):
        with open(
            "tests/schemas/objects/complex-object-type-schema.json"
        ) as schema_file:
            schema = json.load(schema_file)
        struct_type = from_json_to_spark(schema)
        # expects an StructType with a field array of length 2
        expected_length = 2
        self.assertIsInstance(struct_type, StructType)
        self.assertTrue(len(struct_type.fields) == expected_length)
        self.assertEqual(
            (
                struct_type.fields[struct_type.fieldNames().index("stringValue")]
            ).dataType,
            StringType(),
        )
        # compare type name, because instances will have different content
        complex_object = struct_type.fields[
            struct_type.fieldNames().index("complexObjectValue")
        ]
        self.assertEqual(complex_object.dataType.typeName, StructType().typeName)
        simple_object_data_type = complex_object.dataType
        # Check nested properties
        expected_nested_length = 3
        self.assertTrue(len(simple_object_data_type.fields) == expected_nested_length)

        string_value = simple_object_data_type[
            simple_object_data_type.fieldNames().index("stringValue")
        ]
        self.assertEqual(string_value.dataType, StringType())
        self.assertTrue(string_value.nullable)

        array_value = simple_object_data_type[
            simple_object_data_type.fieldNames().index("arrayValue")
        ]
        self.assertEqual(array_value.dataType, ArrayType(StringType()))
        self.assertTrue(array_value.nullable)

        nested_object_value = simple_object_data_type[
            simple_object_data_type.fieldNames().index("nestedObjectValue")
        ]
        # compare type name, because instances will have different content
        self.assertEqual(nested_object_value.dataType.typeName, StructType.typeName)
        self.assertTrue(nested_object_value.nullable)

    # test simple array
    def test_simple_array_type_schema(self):
        with open("tests/schemas/arrays/simple-array-type-schema.json") as schema_file:
            schema = json.load(schema_file)
        struct_type = from_json_to_spark(schema)
        # expects an StructType with a field array of length 1
        self.assertIsInstance(struct_type, StructType)
        self.assertTrue(len(struct_type.fields) == 1)
        # compare type name, because instances will have different content
        simple_array = struct_type.fields[
            struct_type.fieldNames().index("simpleArrayValue")
        ]
        self.assertEqual(
            simple_array.dataType.typeName, ArrayType(elementType=StringType()).typeName
        )  # ArrayType needs an init paramter
        # Check elementType properties
        self.assertEqual(simple_array.dataType.elementType, DoubleType())

    # test simple array with multiple types
    def test_simple_array_multiple_types_schema(self):
        with open(
            "tests/schemas/arrays/simple-array-multiple-types-schema.json"
        ) as schema_file:
            schema = json.load(schema_file)
        struct_type = from_json_to_spark(schema)
        # expects an StructType with a field array of length 1
        self.assertIsInstance(struct_type, StructType)
        self.assertTrue(len(struct_type.fields) == 1)
        # compare type name, because instances will have different content
        simple_array = struct_type.fields[
            struct_type.fieldNames().index("simpleArrayValue")
        ]
        self.assertEqual(
            simple_array.dataType.typeName, ArrayType(elementType=StringType()).typeName
        )  # ArrayType needs an init paramter
        # Check elementType properties
        self.assertEqual(simple_array.dataType.elementType, StringType())

    # test simple integer array with null type
    def test_simple_integer_array_with_null_type_schema(self):
        with open(
            "tests/schemas/arrays/simple-integer-array-with null-type-schema.json"
        ) as schema_file:
            schema = json.load(schema_file)
        struct_type = from_json_to_spark(schema)
        # expects an StructType with a field array of length 1
        self.assertIsInstance(struct_type, StructType)
        self.assertTrue(len(struct_type.fields) == 1)
        # compare type name, because instances will have different content
        simple_array = struct_type.fields[
            struct_type.fieldNames().index("simpleArrayValue")
        ]
        self.assertEqual(
            simple_array.dataType.typeName, ArrayType(elementType=StringType()).typeName
        )  # ArrayType needs an init paramter
        # Check elementType properties
        self.assertEqual(simple_array.dataType.elementType, StringType())

    # test array containg specific type
    def test_array_contains_type_schema(self):
        with open(
            "tests/schemas/arrays/array-contains-type-schema.json"
        ) as schema_file:
            schema = json.load(schema_file)
        struct_type = from_json_to_spark(schema)
        # expects an StructType with a field array of length 1
        self.assertIsInstance(struct_type, StructType)
        self.assertTrue(len(struct_type.fields) == 1)
        # compare type name, because instances will have different content
        simple_array = struct_type.fields[struct_type.fieldNames().index("arrayValue")]
        self.assertEqual(
            simple_array.dataType.typeName, ArrayType(elementType=StringType()).typeName
        )  # ArrayType needs an init paramter
        # Check elementType properties
        # TODO: fix this test!!!
        self.assertEqual(simple_array.dataType.elementType, StringType())

    # test tuple array that does not allow additional items
    def test_array_no_aditional_items_type_schema(self):
        with open(
            "tests/schemas/arrays/array-no-additional-items-type-schema.json"
        ) as schema_file:
            schema = json.load(schema_file)
        struct_type = from_json_to_spark(schema)
        # expects an StructType with a field array of length 1
        expected_length = 1
        self.assertIsInstance(struct_type, StructType)
        self.assertTrue(len(struct_type.fields) == expected_length)
        # compare type name, because instances will have different content
        simple_tuple = struct_type.fields[struct_type.fieldNames().index("arrayValue")]
        self.assertEqual(simple_tuple.dataType.typeName, StructType().typeName)
        self.assertTrue(simple_tuple.nullable)

        tuple_fields = simple_tuple.dataType
        # Check nested properties
        expected_nested_length = 2
        self.assertTrue(len(tuple_fields.fields) == expected_nested_length)
        self.assertEqual(tuple_fields[0].dataType, LongType())
        self.assertEqual(tuple_fields[1].dataType, StringType())

    # test complex array
    def test_complex_array_type_schema(self):
        with open("tests/schemas/arrays/complex-array-type-schema.json") as schema_file:
            schema = json.load(schema_file)
        struct_type = from_json_to_spark(schema)
        # expects an StructType with a field array of length 1
        self.assertIsInstance(struct_type, StructType)
        self.assertTrue(len(struct_type.fields) == 1)
        # compare type name, because instances will have different content
        complex_array = struct_type.fields[
            struct_type.fieldNames().index("complexArrayValue")
        ]
        self.assertEqual(
            complex_array.dataType.typeName,
            ArrayType(elementType=StringType()).typeName,
        )  # ArrayType needs an init paramter
        # Check elementType properties
        # TODO: fix test
        # self.assertEqual(complexArray.dataType.elementType, DoubleType())

    # test required fields
    def test_required_simple_fields(self):
        with open("tests/schemas/required-simple-fields-schema.json") as schema_file:
            schema = json.load(schema_file)
        struct_type = from_json_to_spark(schema)
        # expects an StructType with a field array of length 4
        expected_length = 4
        self.assertIsInstance(struct_type, StructType)
        self.assertTrue(len(struct_type.fields) == expected_length)

        string_value_type = struct_type.fields[
            struct_type.fieldNames().index("stringValue")
        ]
        self.assertEqual(string_value_type.dataType, StringType())
        self.assertFalse(string_value_type.nullable)

        number_value_type = struct_type.fields[
            struct_type.fieldNames().index("numberValue")
        ]
        self.assertEqual(number_value_type.dataType, DoubleType())
        self.assertFalse(number_value_type.nullable)

        integer_value_type = struct_type.fields[
            struct_type.fieldNames().index("integerValue")
        ]
        self.assertEqual(integer_value_type.dataType, LongType())
        self.assertFalse(integer_value_type.nullable)

        boolean_value_type = struct_type.fields[
            struct_type.fieldNames().index("booleanValue")
        ]
        self.assertEqual(boolean_value_type.dataType, BooleanType())
        self.assertFalse(boolean_value_type.nullable)

    # test required fields
    def test_required_complex_fields(self):
        with open("tests/schemas/required-complex-fields-schema.json") as schema_file:
            schema = json.load(schema_file)
        struct_type = from_json_to_spark(schema)
        # expects an StructType with a field array of length 2
        expected_length = 2
        self.assertIsInstance(struct_type, StructType)
        self.assertTrue(len(struct_type.fields) == expected_length)

        # Test Object
        object_value_type = struct_type.fields[
            struct_type.fieldNames().index("objectValue")
        ]
        self.assertEqual(object_value_type.dataType.typeName, StructType().typeName)
        self.assertFalse(object_value_type.nullable)

        object_value_type_data_type = object_value_type.dataType
        self.assertEqual(len(object_value_type_data_type.fields), 2)

        nested_string_value = object_value_type_data_type[
            object_value_type_data_type.fieldNames().index("nestedStringValue")
        ]
        self.assertEqual(nested_string_value.dataType, StringType())
        self.assertFalse(nested_string_value.nullable)

        nested_number_value = object_value_type_data_type[
            object_value_type_data_type.fieldNames().index("nestedNumberValue")
        ]
        self.assertEqual(nested_number_value.dataType, DoubleType())
        self.assertFalse(nested_number_value.nullable)

        # Test Array
        array_value = struct_type.fields[struct_type.fieldNames().index("arrayValue")]
        self.assertEqual(
            array_value.dataType.typeName, ArrayType(elementType=StructType()).typeName
        )
        self.assertFalse(array_value.nullable)

        # Check elementType properties
        array_item_type = array_value.dataType.elementType
        self.assertEqual(array_item_type.typeName, StructType().typeName)

        required_string_value = array_item_type[
            array_item_type.fieldNames().index("requiredStringValue")
        ]
        self.assertEqual(required_string_value.dataType, StringType())
        self.assertFalse(required_string_value.nullable)

        optional_string_value = array_item_type[
            array_item_type.fieldNames().index("optionalStringValue")
        ]
        self.assertEqual(optional_string_value.dataType, StringType())
        self.assertTrue(optional_string_value.nullable)


if __name__ == "__main__":
    unittest.main()
