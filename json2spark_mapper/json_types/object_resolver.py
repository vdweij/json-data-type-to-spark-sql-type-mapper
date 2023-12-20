import logging
from collections.abc import Callable

from pyspark.sql.types import (
    StructField,
    StructType,
)

from ..json_schema_drafts.drafts import JSON_DRAFTS
from .resolver import AbstractObjectResolver


class DefaultObjectResolver(AbstractObjectResolver):
    """
    The "object" type in JSON Schema was introduced in JSON Schema draft-07.
    This version added several new features and improvements,
    and one of them was the introduction of the "object" type as a top-level type in addition to "array," "string," "number," "integer," and "boolean."
    """

    logger = logging.getLogger(__name__)

    def __init__(self):
        super().__init__(
            "Default Object Resolver",
            {JSON_DRAFTS.draft_7, JSON_DRAFTS.draft_2019_09, JSON_DRAFTS.draft_2020_12},
        )

    def resolve(
        self,
        json_snippet: dict,
        schema_reader_callback: Callable[[dict], StructType] | None,
    ) -> StructField:
        self.logger.debug("Converting object...")
        if schema_reader_callback is None:
            raise ValueError("A callable to a schema reader is required")
        return StructType(schema_reader_callback(json_snippet).fields)


class NoneObjectResolver(AbstractObjectResolver):
    """
    The "object" type in JSON Schema was introduced in JSON Schema draft-07.
    This version added several new features and improvements,
    and one of them was the introduction of the "object" type as a top-level type in addition to "array," "string," "number," "integer," and "boolean."
    """

    logger = logging.getLogger(__name__)

    def __init__(self):
        super().__init__(
            "Default Object Resolver",
            {
                JSON_DRAFTS.draft_0,
                JSON_DRAFTS.draft_1,
                JSON_DRAFTS.draft_2,
                JSON_DRAFTS.draft_3,
                JSON_DRAFTS.draft_4,
                JSON_DRAFTS.draft_5,
                JSON_DRAFTS.draft_6,
            },
        )

    def resolve(
        self,
        json_snippet: dict,
        schema_reader_callback: Callable[[dict], StructType] | None,
    ) -> StructField:
        self.logger.debug("Converting object...")
        raise TypeError(
            "The configured json spec does not support the object type that was introduced in draft-07"
        )
