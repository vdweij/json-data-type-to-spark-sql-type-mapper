from abc import ABC, abstractmethod
from enum import Enum

from pyspark.sql.types import StructField

from ..json_schema_drafts.drafts import JsonDraft


class JsonType(Enum):
    STRING = 1
    INTEGER = 2
    NUMBER = 3
    BOOLEAN = 4
    ARRAY = 5
    OBJECT = 6


class AbstractResolver(ABC):
    def __init__(self, name: str, json_type: JsonType, draft_support: set[JsonDraft]):
        if name is None or type(name) != str or name == "":
            raise ValueError("A resolver requires a name (string)")

        if json_type is None or type(json_type) != JsonType:
            raise ValueError("A resolver requires a json_type (JsonType)")

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

    @abstractmethod
    def resolve(self, json_snippet: str) -> StructField:
        pass

    def supports(self) -> bool:
        return False

    def __str__(self):
        return f"JSON Draft: {self.name}"


# Base abstract implementation


class AbstractStringResolver(AbstractResolver):
    def __init__(self, name: str, draft_support: set[JsonDraft]):
        super().__init__(name, JsonType.STRING, draft_support)


class AbstractIntegerResolver(AbstractResolver):
    def __init__(self, name: str, draft_support: set[JsonDraft]):
        super().__init__(name, JsonType.INTEGER, draft_support)


class AbstractNumberResolver(AbstractResolver):
    def __init__(self, name: str, draft_support: set[JsonDraft]):
        super().__init__(name, JsonType.NUMBER, draft_support)


class AbstractBooleanResolver(AbstractResolver):
    def __init__(self, name: str, draft_support: set[JsonDraft]):
        super().__init__(name, JsonType.BOOLEAN, draft_support)


class AbstractArrayResolver(AbstractResolver):
    def __init__(self, name: str, draft_support: set[JsonDraft]):
        super().__init__(name, JsonType.ARRAY, draft_support)


class AbstractObjectResolver(AbstractResolver):
    def __init__(self, name: str, draft_support: set[JsonDraft]):
        super().__init__(name, JsonType.OBJECT, draft_support)
