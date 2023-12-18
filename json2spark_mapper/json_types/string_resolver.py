import logging

from pyspark.sql.types import (
    DateType,
    StringType,
    StructField,
    TimestampType,
)

from ..json_schema_drafts.drafts import JSON_DRAFTS
from .resolver import AbstractStringResolver


class DefaultStringResolver(AbstractStringResolver):
    logger = logging.getLogger(__name__)

    def __init__(self):
        super().__init__("Defaul String Resolver", JSON_DRAFTS.get_all())

    def resolve(self, json_snippet: dict) -> StructField:
        self.logger.debug("Converting string...")

        field_type = StringType()

        if "format" in json_snippet:  # Need to check whether attribute is present first
            self.logger.debug(f"String format found {json_snippet['format']}")
            if json_snippet["format"] == "date-time":
                field_type = TimestampType()
            elif json_snippet["format"] == "date":
                field_type = DateType()

        return field_type
