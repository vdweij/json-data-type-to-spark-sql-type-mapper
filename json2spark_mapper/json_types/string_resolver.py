import logging

from pyspark.sql.types import (
    DateType,
    StringType,
    StructField,
    TimestampType,
)

from ..json_schema_drafts.drafts import JSON_DRAFTS
from .resolver import AbstractStringResolver, PropertyResolver


class DefaultStringResolver(AbstractStringResolver):
    logger = logging.getLogger(__name__)

    def __init__(self):
        super().__init__("Default String Resolver", JSON_DRAFTS.get_all())

    def resolve(
        self,
        json_snippet: dict,
        property_resolver: PropertyResolver | None = None,
    ) -> StructField:
        self.logger.debug("Converting string...")

        field_type = StringType()

        if "format" in json_snippet:  # Need to check whether attribute is present first
            self.logger.debug(f"String format found {json_snippet['format']}")
            # From draft-00 string type supports the format keyword
            # https://json-schema.org/draft-07/json-schema-release-notes#formats
            # https://json-schema.org/draft/2019-09/release-notes#format-vocabulary
            if json_snippet["format"] == "date-time":
                field_type = TimestampType()
            elif json_snippet["format"] == "date":
                field_type = DateType()

        return field_type
