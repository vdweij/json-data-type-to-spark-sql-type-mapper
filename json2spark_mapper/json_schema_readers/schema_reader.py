import logging
from abc import ABC, abstractmethod

from pyspark.sql.types import (
    ArrayType,
    DataType,
    NullType,
    StringType,
    StructField,
    StructType,
)

from json2spark_mapper.json_types.resolver import JsonType, PropertyResolver

from ..json_schema_drafts.drafts import JSON_DRAFTS, JsonDraft
from ..json_types.resolver_registry import ResolverRegistry


class Reader(ABC):
    def __init__(self, name):
        if name is None or type(name) != str or name == "":
            raise ValueError("A reader requires a name (string)")
        self.name = name

    @abstractmethod
    def read_document(self, json: dict) -> StructType:
        pass


class ResolverAwareReader(Reader, PropertyResolver):
    logger = logging.getLogger(__name__)

    def __init__(self, resolver_registries: set[ResolverRegistry]):
        if (
            resolver_registries is None
            or type(resolver_registries) != set
            or len(resolver_registries) == 0
            or any(
                not isinstance(entry, ResolverRegistry) for entry in resolver_registries
            )
        ):
            raise ValueError(
                "The resolvers registry set should not be empty and should contain ResolverRegistry entries"
            )

        self.resolver_registries = resolver_registries

        super().__init__("Resolver Aware Reader")

    def read_document(
        self,
        json_schema: dict,
        default_json_draft: JsonDraft = JSON_DRAFTS.get_latest(),
        force_json_draft: JsonDraft | None = None,
    ) -> StructType:
        # determine draft to use
        json_draft = self._get_draft_to_use(
            json_schema, default_json_draft, force_json_draft
        )
        self.logger.info(f"Using: {json_draft}")
        # get resolver regsitry for json draft version
        self.resolver_registry = self._get_resolver_registry(json_draft)
        self.logger.info("Resolver registry selected, ready for processing...")

        # process
        return self.resolve_properties(json_schema)

    def resolve_properties(self, json_schema: dict) -> StructType:
        properties = json_schema["properties"]
        fields = []
        for key, value in properties.items():
            self.logger.debug(f"Determining type for: {key}")

            field_type = self.resolve_property_type(value)

            self.logger.debug(f"Resolved key {key} to type {field_type.typeName}")

            nullable = True

            # check whether field is required
            if key in json_schema.get("required", []):
                nullable = False
            #
            # Setting nullable has no effect on the created DataFrame. This would be needed to be done afterwards.
            #
            # By default, when Spark reads a JSON file and infers the schema, it assumes that all fields are nullable.
            # If the actual data in the file contains null values for a field that was inferred as non-nullable,
            # Spark will coerce that field to be nullable, since it cannot guarantee that the field will always be non-null.

            fields.append(StructField(key, field_type, nullable))
        return StructType(fields)

    def resolve_property_type(self, json_snippet: dict) -> DataType:
        field_type = None
        if "type" in json_snippet:
            if isinstance(json_snippet["type"], list):
                # an array of types
                field_type = self._map_multiple_json_types_to_spark_type(json_snippet)
            elif json_snippet["type"] == "string":
                field_type = self.resolver_registry.get_resolver(
                    JsonType.STRING
                ).resolve(json_snippet, self)
            elif json_snippet["type"] == "boolean":
                field_type = self.resolver_registry.get_resolver(
                    JsonType.BOOLEAN
                ).resolve(json_snippet, self)
            elif json_snippet["type"] == "integer":
                field_type = self.resolver_registry.get_resolver(
                    JsonType.INTEGER
                ).resolve(json_snippet, self)
            elif json_snippet["type"] == "number":
                field_type = self.resolver_registry.get_resolver(
                    JsonType.NUMBER
                ).resolve(json_snippet, self)
            elif json_snippet["type"] == "array":
                field_type = self.resolver_registry.get_resolver(
                    JsonType.ARRAY
                ).resolve(json_snippet, self)
            elif json_snippet["type"] == "object":
                field_type = self.resolver_registry.get_resolver(
                    JsonType.OBJECT
                ).resolve(json_snippet, self)
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

    def _map_multiple_json_types_to_spark_type(self, json_snippet):
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
            field_type = self.resolve_property_type(json_snippet["type"][0])
        elif len(json_snippet["type"]) == 2:  # noqa PLR2004
            if json_snippet["type"][0] == "null":
                org_types_value = json_snippet["type"]  # copy original value
                json_snippet["type"] = org_types_value[
                    1
                ]  # temp overwrite type to single type
                field_type = self.resolve_property_type(json_snippet)
                json_snippet["type"] = org_types_value  # copy orginal value back
            elif json_snippet["type"][1] == "null":
                org_types_value = json_snippet["type"]  # copy original value
                json_snippet["type"] = org_types_value[
                    0
                ]  # temp overwrite type to single type
                field_type = self.resolve_property_type(json_snippet)
                json_snippet["type"] = org_types_value  # copy orginal value back
            else:
                # multiple types, only safe type is a string based array
                field_type = StringType()
        else:
            field_type = StringType()

        return field_type

    def _get_draft_to_use(
        self,
        json_doc: dict,
        default_json_draft: JsonDraft,
        force_json_draft: JsonDraft | None = None,
    ) -> JsonDraft:
        if force_json_draft is not None:
            if JSON_DRAFTS.contains(force_json_draft):
                json_draft = force_json_draft
                self.logger.debug(f"Forcing draft: {json_draft}")
            else:
                raise ValueError(f"Invalid force draft version: {force_json_draft}")
        elif "$schema" in json_doc:
            # get the version by url
            json_draft = JSON_DRAFTS.get_by_schema_url(json_doc["$schema"])
            self.logger.debug(f"Using JSON document's draft: {json_draft}")
        elif JSON_DRAFTS.contains(default_json_draft):
            # use default
            json_draft = default_json_draft
            self.logger.debug(f"Using default draft: {json_draft}")
        else:
            raise ValueError(f"Invalid default draft version: {default_json_draft}")

        return json_draft

    def _get_resolver_registry(self, json_draft: JsonDraft) -> ResolverRegistry:
        for resolver_registry in self.resolver_registries:
            if resolver_registry.supports(json_draft):
                return resolver_registry

        raise ValueError(
            f"Reader does not have a resolver that supports {json_draft}. Please instantiate a Reader that supports the missing draft."
        )
