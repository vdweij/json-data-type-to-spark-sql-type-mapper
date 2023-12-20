import logging
from collections.abc import Callable

from pyspark.sql.types import BooleanType, StructField, StructType

from ..json_schema_drafts.drafts import JSON_DRAFTS
from .resolver import AbstractBooleanResolver


class DefaultBooleanResolver(AbstractBooleanResolver):
    logger = logging.getLogger(__name__)

    def __init__(self):
        super().__init__("Default Boolean Resolver", JSON_DRAFTS.get_all())

    def resolve(
        self,
        json_snippet: dict,
        schema_reader_callback: Callable[[dict], StructType] | None = None,
    ) -> StructField:
        self.logger.debug("Converting boolean...")

        return BooleanType()
