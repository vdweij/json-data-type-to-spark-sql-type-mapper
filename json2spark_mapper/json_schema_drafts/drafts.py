# https://json-schema.org/specification-links


class JsonDraft:
    __slots__ = (
        "_name",
        "_meta_id",
        "_schema_url",
    )

    def __init__(self, name: str, meta_id: str, schema_url: str | None):
        if name is None or type(name) != str or name == "":
            raise ValueError("JsonDraft requires a name (string)")

        if meta_id is None or type(meta_id) != str or meta_id == "":
            raise ValueError("JsonDraft requires a meta_id (string)")

        if schema_url is not None and (type(schema_url) != str or schema_url == ""):
            raise ValueError("JsonDraft requires a schema_url (string) when specified")

        self._name = name
        self._meta_id = meta_id
        self._schema_url = schema_url

    def __eq__(self, other):
        if isinstance(other, JsonDraft):
            return (
                self._name == other._name
                and self._meta_id == other._meta_id
                and self._schema_url == other._schema_url
            )
        return False

    def __hash__(self):
        return hash(self._name + self._meta_id + str(self._schema_url))

    def __str__(self):
        return f"JSON Draft: {self._name}"

    def contains_url(self, url: str) -> bool:
        # from 2019-09 all meta-schema URIs use https, but http is supported too
        # this check should ignore a differences in http(s)
        # also fragments are allowed in a schema (https://json-schema.org/understanding-json-schema/structuring#schema-identification)
        if self._schema_url is not None:
            self_url_parts = self._schema_url.split("://")
            other_url_parts = url.split("://")

            if self_url_parts[1] == other_url_parts[1]:
                return True
            else:
                return other_url_parts[1].startswith(self_url_parts[1] + "#")
        else:
            return self._schema_url == url

    @property
    def name(self):
        return self._name

    @property
    def meta_id(self):
        return self._meta_id

    @property
    def schema_url(self):
        return self._schema_url


class SupportedJsonDrafts:
    draft_0 = JsonDraft(name="Draft 0", meta_id="draft-00", schema_url=None)
    draft_1 = JsonDraft(
        name="Draft 1",
        meta_id="draft-01",
        schema_url="http://json-schema.org/draft-01/schema",
    )
    draft_2 = JsonDraft(
        name="Draft 2",
        meta_id="draft-02",
        schema_url="http://json-schema.org/draft-02/schema",
    )
    draft_3 = JsonDraft(
        name="Draft 3",
        meta_id="draft-03",
        schema_url="http://json-schema.org/draft-03/schema",
    )
    draft_4 = JsonDraft(
        name="Draft 4",
        meta_id="draft-04",
        schema_url="http://json-schema.org/draft-04/schema",
    )
    draft_5 = JsonDraft(
        name="Draft 5",
        meta_id="draft-05",
        schema_url="http://json-schema.org/draft-05/schema",
    )
    draft_6 = JsonDraft(
        name="Draft 6",
        meta_id="draft-06",
        schema_url="http://json-schema.org/draft-06/schema",
    )
    draft_7 = JsonDraft(
        name="Draft 7",
        meta_id="draft-07",
        schema_url="http://json-schema.org/draft-07/schema",
    )
    draft_2019_09 = JsonDraft(
        name="Draft 2019-09",
        meta_id="2019-09",
        schema_url="http://json-schema.org/draft/2019-09/schema",
    )
    draft_2020_12 = JsonDraft(
        name="Draft 2020-12",
        meta_id="2012-12",
        schema_url="https://json-schema.org/draft/2020-12/schema",
    )

    def get_latest(self) -> JsonDraft:
        return self.draft_2020_12

    def get_all(self) -> set[JsonDraft]:
        return {
            self.draft_0,
            self.draft_1,
            self.draft_2,
            self.draft_3,
            self.draft_4,
            self.draft_5,
            self.draft_6,
            self.draft_7,
            self.draft_2019_09,
            self.draft_2020_12,
        }

    def contains(self, json_draft: JsonDraft) -> bool:
        if (
            self.draft_0 == json_draft
            or self.draft_1 == json_draft
            or self.draft_2 == json_draft
            or self.draft_3 == json_draft
            or self.draft_4 == json_draft
            or self.draft_5 == json_draft
            or self.draft_6 == json_draft
            or self.draft_7 == json_draft
            or self.draft_2019_09 == json_draft
            or self.draft_2020_12 == json_draft
        ):
            return True

        return False

    def get_by_schema_url(self, url: str) -> JsonDraft:
        json_draft = None
        if self.draft_2020_12.contains_url(url):
            json_draft = self.draft_2020_12
        elif self.draft_2019_09.contains_url(url):
            json_draft = self.draft_2019_09
        elif self.draft_7.contains_url(url):
            json_draft = self.draft_7
        elif self.draft_6.contains_url(url):
            json_draft = self.draft_6
        elif self.draft_5.contains_url(url):
            json_draft = self.draft_5
        elif self.draft_4.contains_url(url):
            json_draft = self.draft_4
        elif self.draft_3.contains_url(url):
            json_draft = self.draft_3
        elif self.draft_2.contains_url(url):
            json_draft = self.draft_2
        elif self.draft_1.contains_url(url):
            json_draft = self.draft_1
        elif self.draft_0.contains_url(url):
            json_draft = self.draft_0
        else:
            raise ValueError(f"Invalid schema url: {url}")
        return json_draft


JSON_DRAFTS = SupportedJsonDrafts()
