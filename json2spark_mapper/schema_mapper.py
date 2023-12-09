import logging

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
    StructField,
    StructType,
    TimestampType,
)

logger = logging.getLogger(__name__)


def from_json_to_spark(schema) -> StructType:
    properties = schema["properties"]
    fields = []
    for key, value in properties.items():
        logger.debug(f"Determining type for: {key}")

        field_type = _map_json_type_to_spark_type(value)

        logger.debug(f"Resolved key {key} to type {field_type.typeName}")

        nullable = True

        # check whether field is required
        if key in schema.get("required", []):
            nullable = False
        #
        # Setting nullable has no effect on the created DataFrame. This would be needed to be done afterwards.
        #
        # By default, when Spark reads a JSON file and infers the schema, it assumes that all fields are nullable.
        # If the actual data in the file contains null values for a field that was inferred as non-nullable,
        # Spark will coerce that field to be nullable, since it cannot guarantee that the field will always be non-null.

        fields.append(StructField(key, field_type, nullable))
    return StructType(fields)


def _map_json_type_to_spark_type(json_snippet):
    field_type = None
    if "type" in json_snippet:
        if isinstance(json_snippet["type"], list):
            # an array of types
            field_type = _map_multiple_json_types_to_spark_type(json_snippet)
        elif json_snippet["type"] == "string":
            field_type = _convert_json_string(json_snippet)
        elif json_snippet["type"] == "boolean":
            field_type = BooleanType()
        elif json_snippet["type"] == "integer":
            # This is tricky as there are many Spark types that can be mapped to an int
            field_type = _convert_json_int(json_snippet)
        elif json_snippet["type"] == "number":
            # This is also tricky as there are many Spark types that can be mapped to a number
            field_type = _convert_json_number(json_snippet)
        elif json_snippet["type"] == "array":
            field_type = _convert_json_array(json_snippet)
        elif json_snippet["type"] == "object":
            logger.debug("Converting object...")
            field_type = StructType(from_json_to_spark(json_snippet).fields)
        elif json_snippet["type"] == "null":
            field_type = NullType()
        elif json_snippet["type"] == "any":
            field_type = StringType()
        else:
            raise ValueError(f"Invalid JSON type: {json_snippet['type']}")

    # anyOf is not a type but also a keyword
    elif "anyOf" in json_snippet:
        # A constant can hold all sorts of data types, even complex structures. The savest Spark data type is a StringType.
        # TODO: in case null is the only additional type the other type could be used as field type
        # For instance "anyOf": [{ "type": "null" }, { "type": "number" }] } could be DoubleType with an optional value.
        field_type = StringType()
    # const is not a type but also a keyword
    elif "const" in json_snippet:
        # A constant can hold all sorts of data types, even complex structures. The savest Spark data type is a StringType.
        field_type = StringType()

    return field_type


def _map_multiple_json_types_to_spark_type(json_snippet):
    # Tricky business: https://cswr.github.io/JsonSchema/spec/multiple_types/
    #
    # There are various cases in which multiple types make sense:
    #
    # - It can be used to specify that a property must be present, but that the value can be null
    # - It can be used to indicate that a value can be either one of the specified types.
    #
    # In the latter case a StringType would make sense to use as a SparkType. In the first case
    # the type should be determined via the same path for which a regualar single type property
    # would be used.
    if len(json_snippet["type"]) == 0:
        # only an empty array as value is allowed (weird definition, but valid)
        field_type = ArrayType(StringType())
    elif len(json_snippet["type"]) == 1:
        field_type = _map_json_type_to_spark_type(json_snippet["type"][0])
    elif len(json_snippet["type"]) == 2:  # noqa PLR2004
        if json_snippet["type"][0] == "null":
            org_types_value = json_snippet["type"]  # copy original value
            json_snippet["type"] = org_types_value[
                1
            ]  # temp overwrite type to single type
            field_type = _map_json_type_to_spark_type(json_snippet)
            json_snippet["type"] = org_types_value  # copy orginal value back
        elif json_snippet["type"][1] == "null":
            org_types_value = json_snippet["type"]  # copy original value
            json_snippet["type"] = org_types_value[
                0
            ]  # temp overwrite type to single type
            field_type = _map_json_type_to_spark_type(json_snippet)
            json_snippet["type"] = org_types_value  # copy orginal value back
        else:
            # multiple types, only safe type is a string based array
            field_type = StringType()
    else:
        field_type = StringType()

    return field_type


def _convert_json_string(value):
    logger.debug("Converting string...")

    field_type = StringType()

    if "format" in value:  # Need to check whether attribute is present first
        logger.debug(f"String format found {value['format']}")
        if value["format"] == "date-time":
            field_type = TimestampType()
        elif value["format"] == "date":
            field_type = DateType()

    return field_type


def _convert_json_int(value):
    # This is tricky as there are many Spark types that can be mapped to an int
    #
    # ByteType: Represents 1-byte signed integer numbers. The range of numbers is from -128 to 127.
    # ShortType: Represents 2-byte signed integer numbers. The range of numbers is from -32768 to 32767.
    # IntegerType: Represents 4-byte signed integer numbers. The range of numbers is from -2147483648 to 2147483647.
    # LongType: Represents 8-byte signed integer numbers. The range of numbers is from -9223372036854775808 to 9223372036854775807.
    #
    # https://spark.apache.org/docs/latest/sql-ref-datatypes.html
    #
    # For instance 20230214110547 fits in a json int, but not in a Spark IntegerType
    #
    logger.debug("Converting integer...")

    field_type = LongType()
    determined_range = _determine_inclusive_range(value)
    if determined_range["defined"]:
        # max value of range is exclusive
        byte_type_range = range(-128, 127 + 1)
        short_type_range = range(-32768, 32767 + 1)
        int_type_range = range(-2147483648, 2147483647 + 1)

        if (
            determined_range["min"] in byte_type_range
            and determined_range["max"] in byte_type_range
        ):
            field_type = ByteType()
        elif (
            determined_range["min"] in short_type_range
            and determined_range["max"] in short_type_range
        ):
            field_type = ShortType()
        elif (
            determined_range["min"] in int_type_range
            and determined_range["max"] in int_type_range
        ):
            field_type = IntegerType()

    return field_type


def _convert_json_number(value):
    # This is also tricky as there are many Spark types that can be mapped to a number
    #
    # - FloatType: Represents 4-byte single-precision floating point numbers.
    # - DoubleType: Represents 8-byte double-precision floating point numbers.
    #
    # And optionally
    # - DecimalType: Represents arbitrary-precision signed decimal numbers. Backed internally by java.math.BigDecimal.
    #   A BigDecimal consists of an arbitrary precision integer unscaled value and a 32-bit integer scale.
    #
    # https://spark.apache.org/docs/latest/sql-ref-datatypes.html
    #
    #
    field_type = DoubleType()
    # There is no way to know to purpose of the value. To be on the safe side use DoubleType
    return field_type


def _determine_inclusive_range(value):
    range = {"min": None, "max": None, "defined": False}

    if "minimum" in value:
        range["min"] = int(value["minimum"])
    if "exclusiveMinimum" in value:
        range["min"] = int(value["exclusiveMinimum"]) - 1
    if "maximum" in value:
        range["max"] = int(value["maximum"])
    if "exclusiveMaximum" in value:
        range["max"] = int(value["exclusiveMaximum"]) - 1

    if range["min"] is not None and range["max"] is not None:
        range["defined"] = True

    return range


def _convert_json_array(value):
    # Convert JSON array with either equal-types or different types.
    #
    # JSON arrays com in two forms; the first is a regular array containing a collections of elements of a specific type.
    # The second is the tuple in which elements at different indexes can have different types.
    #
    # The regular array maps perfectly to the Spark ArrayType. The Tuple coudld be represented by a
    # StructType with 'nameless' fields. Spark does create names following a pattern of "col1," "col2," and so on,
    # based on the index of the field within the schema.

    logger.debug("Converting array...")

    if "items" in value:
        items_schemas = value["items"]

        # Check for a dictionary or list (array) of types
        if isinstance(items_schemas, dict):
            field_type = _convert_regular_json_array(items_schemas)
        elif isinstance(items_schemas, list):
            logger.debug("Tuple detected...")
            # This is an array containing a tuple
            # Check whether it has a additionalItems property
            if "additionalItems" in value and value["additionalItems"] is False:
                field_type = _convert_tuple_json_array(items_schemas)
            else:
                # This tuple can contain more types than specified. It's only safe to return a string based array
                logger.debug("Tuple can contain more than specified types.")
                field_type = ArrayType(StringType())
        else:
            raise Exception(
                f"Expected a least one type definition in an array: {value}"
            )

    elif "contains" in value:
        # JSON array is can contain whatever types, but should at least contain a specific type.
        # This type is irrelevant because all other types need to fit the array. The only option
        # is to map it to a string based array.
        field_type = ArrayType(StringType())
    else:
        # Unable to map array type, or should a string based array be returned instead?
        raise ValueError(f"Invalid array definition: {value}")

    return field_type


def _convert_regular_json_array(items_schemas):
    if items_schemas["type"] == "object":
        field_type = ArrayType(StructType(from_json_to_spark(items_schemas).fields))
    elif isinstance(items_schemas["type"], list):
        # a multiple type array
        field_type = ArrayType(_map_multiple_json_types_to_spark_type(items_schemas))
    elif isinstance(items_schemas["type"], str):
        # This is regular array containing a single type
        field_type = ArrayType(_map_json_type_to_spark_type(items_schemas))
    else:
        raise Exception(f"Unexpected type value: {items_schemas}")

    return field_type


def _convert_tuple_json_array(items_schemas):
    # Loop over item schemas and store type as StructType field

    struct_type_fields = []
    for item_schema in items_schemas:
        if item_schema["type"] == "object":
            logger.debug("Tuple items type is an object.")
            tuple_field_type = StructType(from_json_to_spark(item_schema).fields)
        else:
            tuple_field_type = _map_json_type_to_spark_type(item_schema)

        # TODO: check nullable
        nullable = True
        field_name = ""  # Spark will assign col1, col2 etc
        struct_type_fields.append(StructField(field_name, tuple_field_type, nullable))
    field_type = StructType(struct_type_fields)

    return field_type
