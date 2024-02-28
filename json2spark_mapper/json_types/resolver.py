from abc import ABC, abstractmethod
from enum import Enum

from pyspark.sql.types import DataType, StructField, StructType

from ..json_schema_drafts.drafts import JsonDraft


class JsonType(Enum):
    STRING = 1
    INTEGER = 2
    NUMBER = 3
    BOOLEAN = 4
    ARRAY = 5
    OBJECT = 6


class PropertyResolver(ABC):
    @abstractmethod
    def resolve_properties(self, json_snippet: dict) -> StructType:
        pass

    @abstractmethod
    def resolve_property_type(self, json_snippet: dict) -> DataType:
        pass


class TypeResolver(ABC):
    def __init__(self, name: str, json_type: JsonType, draft_support: set[JsonDraft]):
        if name is None or type(name) != str or name == "":
            raise ValueError("A type resolver requires a name (string)")

        if json_type is None or type(json_type) != JsonType:
            raise ValueError("A type resolver requires a json_type (JsonType)")

        if (
            draft_support is None
            or type(draft_support) != set
            or len(draft_support) == 0
            or any(not isinstance(entry, JsonDraft) for entry in draft_support)
        ):
            raise ValueError(
                "The draft_support set should not be empty and should contain JsonDraft entries"
            )

        self.name = name
        self.json_type = json_type
        self.draft_support = draft_support

    @abstractmethod
    def resolve(
        self,
        json_snippet: dict,
        property_resolver: PropertyResolver | None,
    ) -> StructField:
        pass

    @abstractmethod
    def supports_json_snippet(self, json_snippet: dict) -> bool:
        pass

    def supports(self, json_type: JsonType, json_draft: JsonDraft) -> bool:
        return self.supports_json_type(json_type) and self.supports_json_draft(
            json_draft
        )

    def supports_json_type(self, json_type: JsonType) -> bool:
        return self.json_type == json_type

    def supports_json_draft(self, json_draft: JsonDraft) -> bool:
        return json_draft in self.draft_support

    def __str__(self):
        return f"JSON Draft: {self.name}"


# Base abstract implementation
class AbstractStringResolver(TypeResolver):
    def __init__(self, name: str, draft_support: set[JsonDraft]):
        super().__init__(name, JsonType.STRING, draft_support)

    def supports_json_snippet(self, json_snippet: dict) -> bool:
        if "type" in json_snippet and json_snippet["type"] == "string":
            return True
        else:
            return False


class AbstractIntegerResolver(TypeResolver):
    def __init__(self, name: str, draft_support: set[JsonDraft]):
        super().__init__(name, JsonType.INTEGER, draft_support)

    def supports_json_snippet(self, json_snippet: dict) -> bool:
        if "type" in json_snippet and json_snippet["type"] == "integer":
            return True
        else:
            return False


class AbstractNumberResolver(TypeResolver):
    def __init__(self, name: str, draft_support: set[JsonDraft]):
        super().__init__(name, JsonType.NUMBER, draft_support)

    def supports_json_snippet(self, json_snippet: dict) -> bool:
        if "type" in json_snippet and json_snippet["type"] == "number":
            return True
        else:
            return False


class AbstractBooleanResolver(TypeResolver):
    def __init__(self, name: str, draft_support: set[JsonDraft]):
        super().__init__(name, JsonType.BOOLEAN, draft_support)

    def supports_json_snippet(self, json_snippet: dict) -> bool:
        if "type" in json_snippet and json_snippet["type"] == "boolean":
            return True
        else:
            return False


class AbstractArrayResolver(TypeResolver):
    def __init__(self, name: str, draft_support: set[JsonDraft]):
        super().__init__(name, JsonType.ARRAY, draft_support)

    def supports_json_snippet(self, json_snippet: dict) -> bool:
        if "type" in json_snippet and json_snippet["type"] == "array":
            return True
        else:
            return False


class AbstractObjectResolver(TypeResolver):
    def __init__(self, name: str, draft_support: set[JsonDraft]):
        super().__init__(name, JsonType.OBJECT, draft_support)

    def supports_json_snippet(self, json_snippet: dict) -> bool:
        if "type" in json_snippet and json_snippet["type"] == "object":
            return True
        else:
            return False
