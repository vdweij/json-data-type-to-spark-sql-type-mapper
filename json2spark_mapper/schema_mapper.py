import logging

from pyspark.sql.types import (
    ArrayType,
    NullType,
    StringType,
    StructField,
    StructType,
)

from json2spark_mapper.json_types.object_resolver import DefaultObjectResolver

from .json_schema_drafts.drafts import JSON_DRAFTS, JsonDraft
from .json_types.boolean_resolver import DefaultBooleanResolver
from .json_types.integer_resolver import Draft3OnwardsIntegerResolver
from .json_types.number_resolver import DefaultNumberResolver
from .json_types.string_resolver import DefaultStringResolver

logger = logging.getLogger(__name__)


def from_json_to_spark(
    schema,
    default_json_draft: JsonDraft = JSON_DRAFTS.draft_2020_12,
    force_json_draft: JsonDraft | None = None,
) -> StructType:
    """
    Converts a Json schema into a Spark StructType.

    Parameters:
        schema: The json schema to convert.
        default_json_draft (JsonDraft, optional): In case the json schema has no explicit version it can be set via this parameter. Defaults to "Draft 2020-12".
        force_json_draft (JsonDraft, optional): In case a specific draft version needs to be used, ignoring the version specified in the json schema or provided via default_json_draft. Defaults to None.

    Returns:
        StructType
    """

    if force_json_draft is not None:
        if JSON_DRAFTS.contains(force_json_draft):
            json_draft = force_json_draft
            logger.debug(f"Forcing draft: {json_draft}")
        else:
            raise ValueError(f"Invalid force draft version: {force_json_draft}")
    elif "$version" in schema:
        # get the version by url
        json_draft = JSON_DRAFTS.get_by_schema_url(schema["$version"])
        logger.debug(f"Using JSON document's draft: {json_draft}")
    elif JSON_DRAFTS.contains(default_json_draft):
        # use default
        json_draft = default_json_draft
        logger.debug(f"Using default draft: {json_draft}")
    else:
        raise ValueError(f"Invalid default draft version: {default_json_draft}")

    logger.info(f"Using: {json_draft}")

    return _from_json_to_spark(schema, json_draft)


def _from_json_to_spark(schema, json_draft: JsonDraft | None = None) -> StructType:
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
            field_type = DefaultBooleanResolver().resolve(json_snippet)
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
            field_type = DefaultObjectResolver().resolve(
                json_snippet, _from_json_to_spark
            )
            field_type = StructType(_from_json_to_spark(json_snippet).fields)
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
    return DefaultStringResolver().resolve(value)


def _convert_json_int(value):
    return Draft3OnwardsIntegerResolver().resolve(value)


def _convert_json_number(value):
    return DefaultNumberResolver().resolve(value)


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
        field_type = ArrayType(StructType(_from_json_to_spark(items_schemas).fields))
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
            tuple_field_type = StructType(_from_json_to_spark(item_schema).fields)
        else:
            tuple_field_type = _map_json_type_to_spark_type(item_schema)

        # TODO: check nullable
        nullable = True
        field_name = ""  # Spark will assign col1, col2 etc
        struct_type_fields.append(StructField(field_name, tuple_field_type, nullable))
    field_type = StructType(struct_type_fields)

    return field_type
