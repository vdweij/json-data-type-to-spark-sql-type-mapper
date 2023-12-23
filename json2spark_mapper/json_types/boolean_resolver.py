import logging

from pyspark.sql.types import BooleanType, StructField

from ..json_schema_drafts.drafts import JSON_DRAFTS
from .resolver import AbstractBooleanResolver, PropertyResolver


class DefaultBooleanResolver(AbstractBooleanResolver):
    logger = logging.getLogger(__name__)

    def __init__(self):
        super().__init__("Default Boolean Resolver", JSON_DRAFTS.get_all())

    def resolve(
        self,
        json_snippet: dict,
        property_resolver: PropertyResolver | None,
    ) -> StructField:
        self.logger.debug("Converting boolean...")

        return BooleanType()
