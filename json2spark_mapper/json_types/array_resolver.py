import logging

from pyspark.sql.types import ArrayType, StringType, StructField, StructType

from ..json_schema_drafts.drafts import JSON_DRAFTS
from .resolver import AbstractArrayResolver, PropertyResolver


class DefaultArrayResolver(AbstractArrayResolver):
    logger = logging.getLogger(__name__)

    def __init__(self):
        super().__init__("Default Array Resolver", JSON_DRAFTS.get_all())

    def resolve(
        self,
        json_snippet: dict,
        property_resolver: PropertyResolver | None,
    ) -> StructField:
        self.logger.debug("Converting array...")
        if property_resolver is None:
            raise ValueError("A property resolver is required")
        # Convert JSON array with either equal-types or different types.
        #
        # JSON arrays com in two forms; the first is a regular array containing a collections of elements of a specific type.
        # The second is the tuple in which elements at different indexes can have different types.
        #
        # The regular array maps perfectly to the Spark ArrayType. The Tuple coudld be represented by a
        # StructType with 'nameless' fields. Spark does create names following a pattern of "col1," "col2," and so on,
        # based on the index of the field within the schema.

        if "items" in json_snippet:
            items_schemas = json_snippet["items"]

            # Check for a dictionary or list (array) of types
            if isinstance(items_schemas, dict):
                field_type = self._convert_regular_json_array(
                    items_schemas, property_resolver
                )
            elif isinstance(items_schemas, list):
                self.logger.debug("Tuple detected...")
                # This is an array containing a tuple
                # Check whether it has a additionalItems property
                if (
                    "additionalItems" in json_snippet
                    and json_snippet["additionalItems"] is False
                ):
                    field_type = self._convert_tuple_json_array(
                        items_schemas, property_resolver
                    )
                else:
                    # This tuple can contain more types than specified. It's only safe to return a string based array
                    self.logger.debug("Tuple can contain more than specified types.")
                    field_type = ArrayType(StringType())
            else:
                raise Exception(
                    f"Expected a least one type definition in an array: {json_snippet}"
                )

        elif "contains" in json_snippet:
            # JSON array is can contain whatever types, but should at least contain a specific type.
            # This type is irrelevant because all other types need to fit the array. The only option
            # is to map it to a string based array.
            field_type = ArrayType(StringType())
        else:
            # Unable to map array type, or should a string based array be returned instead?
            raise ValueError(f"Invalid array definition: {json_snippet}")

        return field_type

    def _convert_regular_json_array(
        self, items_schemas, property_resolver: PropertyResolver
    ):
        if items_schemas["type"] == "object":
            element_type = StructType(
                property_resolver.resolve_properties(items_schemas).fields
            )
        else:
            # This is regular array containing a single type
            element_type = property_resolver.resolve_property_type(items_schemas)

        return ArrayType(element_type)

    def _convert_tuple_json_array(
        self, items_schemas, property_resolver: PropertyResolver
    ):
        # Loop over item schemas and store type as StructType field

        struct_type_fields = []
        for item_schema in items_schemas:
            if "type" in item_schema and item_schema["type"] == "object":
                self.logger.debug("Tuple items type is an object.")
                tuple_field_type = StructType(
                    property_resolver.resolve_properties(item_schema).fields
                )
            else:
                tuple_field_type = property_resolver.resolve_property_type(item_schema)

            # TODO: check nullable
            nullable = True
            field_name = ""  # Spark will assign col1, col2 etc
            struct_type_fields.append(
                StructField(field_name, tuple_field_type, nullable)
            )
        field_type = StructType(struct_type_fields)

        return field_type


class Draft202012ArrayResolver(DefaultArrayResolver):
    """
    In Draft 2020-12 `prefixItems` was introduced to allow tuple validation, meaning that when the array
    is a collection of items where each has a different schema and the ordinal index of each item is meaningful.

    In Draft 4 - 2019-09, tuple validation was handled by an alternate form of the items keyword.
    When items was an array of schemas instead of a single schema, it behaved the way prefixItems behaves.

    Args:
        DefaultArrayResolver (_type_): _description_
    """

    logger = logging.getLogger(__name__)

    def resolve(
        self,
        json_snippet: dict,
        property_resolver: PropertyResolver | None,
    ) -> StructField:
        self.logger.debug("Converting array...")
        if property_resolver is None:
            raise ValueError("A property resolver is required")

        if "prefixItems" in json_snippet:
            self.logger.debug("Tuple detected...")
            # need to find out whether there are other items allowed
            if "items" not in json_snippet or (
                type(json_snippet["items"]) is bool and json_snippet["items"] is False
            ):
                # only a tuple
                items_schemas = json_snippet["prefixItems"]

                field_type = self._convert_tuple_json_array(
                    items_schemas, property_resolver
                )
            else:
                # a tuple but any other type can follow, hence the only safe type to chooce is StringType
                self.logger.debug("Tuple can contain more than specified types.")
                field_type = ArrayType(StringType())
        elif "items" in json_snippet and type(json_snippet) is dict:
            # a regular list
            items_schemas = json_snippet["items"]

            # check if type is a dict an not a list
            if type(items_schemas) is dict:
                field_type = self._convert_regular_json_array(
                    items_schemas, property_resolver
                )
            else:
                raise ValueError(
                    f"Draft 2020-12 only allows for a boolean or single type item value in case of a 'tuple'. A {type(items_schemas)} is unsupported."
                )
        elif "contains" in json_snippet:
            self.logger.debug("Contains detected... other types could be present")
            # this means a type must appear in the array, but others are also allowed, hence the only safe type to chooce is StringType
            field_type = ArrayType(StringType())

        # TODO:
        # https://json-schema.org/understanding-json-schema/reference/array#unevaluateditems

        return field_type
