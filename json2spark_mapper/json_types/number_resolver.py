import logging

from pyspark.sql.types import DoubleType, StructField

from ..json_schema_drafts.drafts import JSON_DRAFTS
from .resolver import AbstractNumberResolver, PropertyResolver


class DefaultNumberResolver(AbstractNumberResolver):
    logger = logging.getLogger(__name__)

    def __init__(self):
        super().__init__("Default Number Resolver", JSON_DRAFTS.get_all())

    def resolve(
        self,
        json_snippet: dict,
        property_resolver: PropertyResolver | None,
    ) -> StructField:
        self.logger.debug("Converting number...")

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
