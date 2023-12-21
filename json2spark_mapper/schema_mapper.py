import logging
from collections.abc import Callable

from pyspark.sql.types import (
    StructField,
    StructType,
)

from json2spark_mapper.json_schema_readers.schema_reader import ResolverAwareReader
from json2spark_mapper.json_types.object_resolver import DefaultObjectResolver
from json2spark_mapper.json_types.resolver import AbstractArrayResolver
from json2spark_mapper.json_types.resolver_registry import ResolverRegistryBuilder

from .json_schema_drafts.drafts import JSON_DRAFTS, JsonDraft
from .json_types.boolean_resolver import DefaultBooleanResolver
from .json_types.integer_resolver import Draft3OnwardsIntegerResolver
from .json_types.number_resolver import DefaultNumberResolver
from .json_types.string_resolver import DefaultStringResolver

logger = logging.getLogger(__name__)


class DummyArrayResolver(AbstractArrayResolver):
    logger = logging.getLogger(__name__)

    def __init__(self):
        super().__init__("Dummy Array Resolver", JSON_DRAFTS.get_all())

    def resolve(
        self,
        json_snippet: dict,
        schema_reader_callback: Callable[[dict], StructType] | None = None,
    ) -> StructField:
        self.logger.debug("Converting array...")
        raise NotImplementedError("Dummy array resolver is just dummy...")


# create some registries
registry_draft_2020_12 = (
    ResolverRegistryBuilder(JSON_DRAFTS.draft_2020_12)
    .addStringResolver(DefaultStringResolver())
    .addIntegerResolver(Draft3OnwardsIntegerResolver())
    .addNumberResolver(DefaultNumberResolver())
    .addBooleanResolver(DefaultBooleanResolver())
    .addObjectResolver(DefaultObjectResolver())
    .addArrayResolver(DummyArrayResolver())
    .build()
)
registry_draft_2019_09 = registry_draft_2020_12.copy(JSON_DRAFTS.draft_2019_09)

# instantiate a reader
reader = ResolverAwareReader({registry_draft_2020_12, registry_draft_2019_09})


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

    return reader.read_document(schema, default_json_draft, force_json_draft)
