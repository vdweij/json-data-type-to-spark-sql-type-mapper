import logging

from pyspark.sql.types import (
    StructType,
)

from json2spark_mapper.json_schema_readers.schema_reader import ResolverAwareReader
from json2spark_mapper.json_types.array_resolver import (
    DefaultArrayResolver,
    Draft202012ArrayResolver,
)
from json2spark_mapper.json_types.object_resolver import (
    DefaultObjectResolver,
    NoneObjectResolver,
)
from json2spark_mapper.json_types.resolver_registry import ResolverRegistryBuilder

from .json_schema_drafts.drafts import JSON_DRAFTS, JsonDraft
from .json_types.boolean_resolver import DefaultBooleanResolver
from .json_types.integer_resolver import (
    Draft0ToDraft2IntegerResolver,
    Draft3OnwardsIntegerResolver,
)
from .json_types.number_resolver import DefaultNumberResolver
from .json_types.string_resolver import DefaultStringResolver

logger = logging.getLogger(__name__)

# create registries

registry_draft_0 = (
    ResolverRegistryBuilder(JSON_DRAFTS.draft_0)
    .addStringResolver(DefaultStringResolver())
    .addIntegerResolver(Draft0ToDraft2IntegerResolver())
    .addNumberResolver(DefaultNumberResolver())
    .addBooleanResolver(DefaultBooleanResolver())
    .addObjectResolver(NoneObjectResolver())
    .addArrayResolver(DefaultArrayResolver())
    .build()
)
registry_draft_1 = registry_draft_0.copy_to(JSON_DRAFTS.draft_1)
registry_draft_2 = registry_draft_1.copy_to(JSON_DRAFTS.draft_2)

registry_draft_3 = (
    ResolverRegistryBuilder(JSON_DRAFTS.draft_3)
    .addStringResolver(DefaultStringResolver())
    .addIntegerResolver(
        Draft3OnwardsIntegerResolver()
    )  # <-- different way of including min and max
    .addNumberResolver(DefaultNumberResolver())
    .addBooleanResolver(DefaultBooleanResolver())
    .addObjectResolver(NoneObjectResolver())
    .addArrayResolver(DefaultArrayResolver())
    .build()
)
registry_draft_4 = registry_draft_3.copy_to(JSON_DRAFTS.draft_4)
registry_draft_5 = registry_draft_4.copy_to(JSON_DRAFTS.draft_5)
registry_draft_6 = registry_draft_5.copy_to(JSON_DRAFTS.draft_6)

registry_draft_7 = (
    ResolverRegistryBuilder(JSON_DRAFTS.draft_7)
    .addStringResolver(DefaultStringResolver())
    .addIntegerResolver(
        Draft3OnwardsIntegerResolver()
    )  # <-- different way of including min and max
    .addNumberResolver(DefaultNumberResolver())
    .addBooleanResolver(DefaultBooleanResolver())
    .addObjectResolver(DefaultObjectResolver())
    .addArrayResolver(DefaultArrayResolver())
    .build()
)

registry_draft_2019_09 = registry_draft_7.copy_to(JSON_DRAFTS.draft_2019_09)

registry_draft_2020_12 = (
    ResolverRegistryBuilder(JSON_DRAFTS.draft_2020_12)
    .addStringResolver(DefaultStringResolver())
    .addIntegerResolver(Draft3OnwardsIntegerResolver())
    .addNumberResolver(DefaultNumberResolver())
    .addBooleanResolver(DefaultBooleanResolver())
    .addObjectResolver(DefaultObjectResolver())
    .addArrayResolver(Draft202012ArrayResolver())  # <-- different way of solving tuples
    .build()
)

# instantiate a reader
READER = ResolverAwareReader(
    {
        registry_draft_2020_12,
        registry_draft_2019_09,
        registry_draft_7,
        registry_draft_6,
        registry_draft_5,
        registry_draft_4,
        registry_draft_3,
        registry_draft_2,
        registry_draft_1,
        registry_draft_0,
    }
)


def from_json_to_spark(
    schema,
    default_json_draft: JsonDraft = JSON_DRAFTS.get_latest(),
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

    return READER.read_document(schema, default_json_draft, force_json_draft)
