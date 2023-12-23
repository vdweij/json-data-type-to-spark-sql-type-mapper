import logging
from abc import abstractmethod

from pyspark.sql.types import (
    ByteType,
    IntegerType,
    LongType,
    ShortType,
    StructField,
)

from ..json_schema_drafts.drafts import JSON_DRAFTS, JsonDraft
from .resolver import AbstractIntegerResolver, PropertyResolver


class Range:
    minimum: bool | None = None
    maximum: bool | None = None

    def is_defined(self) -> bool:
        return self.minimum is not None and self.maximum is not None


class BaseIntegerResolver(AbstractIntegerResolver):
    logger = logging.getLogger(__name__)

    def __init__(self, name: str, draft_support: set[JsonDraft]):
        super().__init__(name, draft_support)

    def resolve(
        self,
        json_snippet: dict,
        property_resolver: PropertyResolver | None,
    ) -> StructField:
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
        self.logger.debug("Converting integer...")

        field_type = LongType()
        determined_range = self.determine_range(json_snippet)
        if determined_range.is_defined():
            # max value of range is exclusive
            byte_type_range = range(-128, 127 + 1)
            short_type_range = range(-32768, 32767 + 1)
            int_type_range = range(-2147483648, 2147483647 + 1)

            if (
                determined_range.minimum in byte_type_range
                and determined_range.maximum in byte_type_range
            ):
                field_type = ByteType()
            elif (
                determined_range.minimum in short_type_range
                and determined_range.maximum in short_type_range
            ):
                field_type = ShortType()
            elif (
                determined_range.minimum in int_type_range
                and determined_range.maximum in int_type_range
            ):
                field_type = IntegerType()

        return field_type

    @abstractmethod
    def determine_range(self, json_snippet: dict) -> Range:
        pass


class Draft0ToDraft2IntegerResolver(BaseIntegerResolver):
    """
    The integer type can have minimum and maximum attribute, but also a
    boolean minimumCanEqual or maximumCanEqual attribute.

    As per specifications (https://json-schema.org/draft-00/draft-zyp-json-schema-00.txt):

    5.7.  minimum

        This indicates the minimum value for the instance property when the
        type of the instance value is a number.

    5.8.  maximum

        This indicates the minimum value for the instance property when the
        type of the instance value is a number.

    5.9.  minimumCanEqual

        If the minimum is defined, this indicates whether or not the instance
        property value can equal the minimum.

    5.10.  maximumCanEqual

        If the maximum is defined, this indicates whether or not the instance
        property value can equal the maximum.

    These specs remained unchanged in:
    https://json-schema.org/draft-01/draft-zyp-json-schema-01
    https://json-schema.org/draft-02/draft-zyp-json-schema-02.txt

    In draft-03 they were replaced (https://json-schema.org/draft-03/draft-zyp-json-schema-03.pdf):
    Replaced "maximumCanEqual" attribute with "exclusiveMaximum" attribute.
    Replaced "minimumCanEqual" attribute with "exclusiveMinimum" attribute.

    """

    def __init__(self):
        super().__init__(
            "Draft01ToDraft02IntegerResolver",
            {JSON_DRAFTS.draft_0, JSON_DRAFTS.draft_1, JSON_DRAFTS.draft_2},
        )

    def determine_range(self, json_snippet):
        range = Range()

        if "minimum" in json_snippet:
            range.minimum = int(json_snippet["minimum"])
            if "minimumCanEqual" in json_snippet:
                range.minimum = range.minimum - 1
        if "maximum" in json_snippet:
            range.maximum = int(json_snippet["maximum"])
            if "maximumCanEqual" in json_snippet:
                range.maximum = range.maximum + 1

        return range


class Draft3OnwardsIntegerResolver(BaseIntegerResolver):
    """
    In draft-03 maximumCanEqual and minimumCanEqual were replaced (https://json-schema.org/draft-03/draft-zyp-json-schema-03.pdf):
    Replaced "maximumCanEqual" attribute with "exclusiveMaximum" attribute.
    Replaced "minimumCanEqual" attribute with "exclusiveMinimum" attribute.

    """

    def __init__(self):
        super().__init__(
            "Draft03OnwardIntegerResolver",
            {
                JSON_DRAFTS.draft_3,
                JSON_DRAFTS.draft_4,
                JSON_DRAFTS.draft_5,
                JSON_DRAFTS.draft_6,
                JSON_DRAFTS.draft_7,
                JSON_DRAFTS.draft_2019_09,
                JSON_DRAFTS.draft_2020_12,
            },
        )

    def determine_range(self, json_snippet):
        range = Range()

        if "minimum" in json_snippet:
            range.minimum = int(json_snippet["minimum"])
        if "exclusiveMinimum" in json_snippet:
            range.minimum = int(json_snippet["exclusiveMinimum"]) - 1
        if "maximum" in json_snippet:
            range.maximum = int(json_snippet["maximum"])
        if "exclusiveMaximum" in json_snippet:
            range.maximum = int(json_snippet["exclusiveMaximum"]) - 1

        return range
