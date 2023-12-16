from ..json_schema_drafts.drafts import JsonDraft
from .resolver import JsonType, Resolver


class ResolverRegistry:
    """
    A ResolverRegsitry holds a set of json type resolvers for a specific json draft version
    """

    def __init__(self, json_draft: JsonDraft, resolvers: set[Resolver]):
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
            or any(not isinstance(entry, Resolver) for entry in resolvers)
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
            if self.get_resolver(json_type) is None:
                raise ValueError(
                    f"No support found for json type {json_type} and draft version {self.json_draft}"
                )

    def supports(self, json_draft: JsonDraft) -> bool:
        return self.json_draft == json_draft

    def get_resolver(self, json_type: JsonType) -> Resolver | None:
        for resolver in self.resolvers:
            if resolver.supports(json_type, self.json_draft):
                return resolver

        return None


class ResolverRegistryBuilder:
    def __init__(self, json_draft: JsonDraft):
        if json_draft is None or type(json_draft) != JsonDraft:
            raise ValueError("A valid json draft should be specified")
        self.json_draft = json_draft
        self.string_resolver: Resolver | None = None
        self.number_resolver: Resolver | None = None
        self.integer_resolver: Resolver | None = None
        self.boolean_resolver: Resolver | None = None
        self.array_resolver: Resolver | None = None
        self.object_resolver: Resolver | None = None

    def addStringResolver(self, resolver: Resolver):
        self._check_type(JsonType.STRING, resolver)
        self.string_resolver = resolver
        return self

    def addIntegerResolver(self, resolver: Resolver):
        self._check_type(JsonType.INTEGER, resolver)
        self.string_resolver = resolver
        return self

    def addNumberResolver(self, resolver: Resolver):
        self._check_type(JsonType.NUMBER, resolver)
        self.string_resolver = resolver
        return self

    def addBooleanResolver(self, resolver: Resolver):
        self._check_type(JsonType.BOOLEAN, resolver)
        self.string_resolver = resolver
        return self

    def addArrayResolver(self, resolver: Resolver):
        self._check_type(JsonType.ARRAY, resolver)
        self.string_resolver = resolver
        return self

    def addObjectResolver(self, resolver: Resolver):
        self._check_type(JsonType.OBJECT, resolver)
        self.string_resolver = resolver
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

    def _check_type(self, json_type, resolver: Resolver):
        # check support
        if not resolver.supports(json_type, self.json_draft):
            raise ValueError(
                f"Cannot add a {json_type} resolver for {self.json_draft}. Resolver supports type {resolver.json_type} and drafts {', '.join(map(str, resolver.draft_support))}"
            )
