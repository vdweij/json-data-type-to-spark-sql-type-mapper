from __future__ import annotations

import logging  # allows using forward references for type hints

from ..json_schema_drafts.drafts import JsonDraft
from .resolver import JsonType, TypeResolver


class ResolverRegistry:
    """
    A ResolverRegsitry holds a set of json type resolvers for a specific json draft version
    """

    def __init__(self, json_draft: JsonDraft, resolvers: set[TypeResolver]):
        """
        Construct the class by passing valid arguments. The set of resolvers should be able to
        resolve every json type as specified in the JsonType enum.

        Args:
            json_draft (JsonDraft): a json draft version
            resolvers (set[Resolver]): a set of resolvers

        Raises:
            ValueError: indicating an invalid json_draft value
            ValueError: indicating an invalid resolvers value
        """
        if json_draft is None or type(json_draft) != JsonDraft:
            raise ValueError("A valid json draft should be specified")

        if (
            resolvers is None
            or type(resolvers) != set
            or len(resolvers) == 0
            or any(not isinstance(entry, TypeResolver) for entry in resolvers)
        ):
            raise ValueError(
                "The resolvers set should not be empty and should contain Resolver entries"
            )

        self.json_draft = json_draft
        self.resolvers = resolvers

        self._validate_resolvers_coverage()

    def _validate_resolvers_coverage(self):
        # Each combination of json type and draft version should be handled by a resolver
        for json_type in JsonType:
            self.get_resolver(json_type)  # in case not found an error is raised

    def supports(self, json_draft: JsonDraft) -> bool:
        return self.json_draft == json_draft

    def get_resolver(self, json_type: JsonType) -> TypeResolver:
        for resolver in self.resolvers:
            if resolver.supports(json_type, self.json_draft):
                return resolver

        raise LookupError(
            f"Could not find a resolver for json type {json_type} in registry"
        )

    def copy_to(self, json_draft: JsonDraft) -> ResolverRegistry:
        return ResolverRegistry(json_draft, self.resolvers)


class ResolverRegistryBuilder:
    logger = logging.getLogger(__name__)

    def __init__(self, json_draft: JsonDraft):
        if json_draft is None or type(json_draft) != JsonDraft:
            raise ValueError("A valid json draft should be specified")
        self.json_draft = json_draft
        self.string_resolver: TypeResolver | None = None
        self.number_resolver: TypeResolver | None = None
        self.integer_resolver: TypeResolver | None = None
        self.boolean_resolver: TypeResolver | None = None
        self.array_resolver: TypeResolver | None = None
        self.object_resolver: TypeResolver | None = None

    def addStringResolver(self, resolver: TypeResolver) -> ResolverRegistryBuilder:
        self._check_type(JsonType.STRING, resolver)
        self.string_resolver = resolver
        return self

    def addIntegerResolver(self, resolver: TypeResolver) -> ResolverRegistryBuilder:
        self._check_type(JsonType.INTEGER, resolver)
        self.integer_resolver = resolver
        return self

    def addNumberResolver(self, resolver: TypeResolver) -> ResolverRegistryBuilder:
        self._check_type(JsonType.NUMBER, resolver)
        self.number_resolver = resolver
        return self

    def addBooleanResolver(self, resolver: TypeResolver) -> ResolverRegistryBuilder:
        self._check_type(JsonType.BOOLEAN, resolver)
        self.boolean_resolver = resolver
        return self

    def addArrayResolver(self, resolver: TypeResolver) -> ResolverRegistryBuilder:
        self._check_type(JsonType.ARRAY, resolver)
        self.array_resolver = resolver
        return self

    def addObjectResolver(self, resolver: TypeResolver) -> ResolverRegistryBuilder:
        self._check_type(JsonType.OBJECT, resolver)
        self.object_resolver = resolver
        return self

    def build(self) -> ResolverRegistry:
        if (
            self.string_resolver is None
            or self.integer_resolver is None
            or self.number_resolver is None
            or self.boolean_resolver is None
            or self.array_resolver is None
            or self.object_resolver is None
        ):
            raise ValueError("Builder not ready yet")

        return ResolverRegistry(
            self.json_draft,
            {
                self.string_resolver,
                self.integer_resolver,
                self.number_resolver,
                self.boolean_resolver,
                self.array_resolver,
                self.object_resolver,
            },
        )

    def _check_type(self, json_type, resolver: TypeResolver):
        # check support
        if not resolver.supports(json_type, self.json_draft):
            raise ValueError(
                f"Cannot add a {json_type} resolver for {self.json_draft}. Resolver supports type {resolver.json_type} and drafts {', '.join(map(str, resolver.draft_support))}"
            )
