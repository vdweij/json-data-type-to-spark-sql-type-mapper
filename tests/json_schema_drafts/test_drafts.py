"""
# For debuging in VSCode add this launch file/snippet in .vscode/launch.json
{
    "version": "0.2.0",
    "configurations": [
        {
            "name": "Python: Unittest",
            "type": "python",
            "request": "launch",
            "program": "${file}",
            "purpose": ["debug-test"],
            "console": "integratedTerminal",
            "env": {"PYTHONPATH": "${workspaceFolder}${pathSeparator}${env:PYTHONPATH}"}
        }
    ]
}

# No longer needed:
# import sys
# sys.path.append(".")
"""
import unittest

from json2spark_mapper.json_schema_drafts.drafts import JsonDraft


class TestDrafts(unittest.TestCase):
    def test_url_ignoring_protocol(self):
        http_url = "http://json-schema.org/draft/2020-12/schema"
        https_url = "https://json-schema.org/draft/2020-12/schema"
        bad_url = "https://json-schema.org/draft/4444-44/schema"
        name = "Draft 2020-12"
        meta_id = "2012-12"
        draft_2019_09 = JsonDraft(name, meta_id, https_url)

        self.assertTrue(draft_2019_09.contains_url(http_url))
        self.assertTrue(draft_2019_09.contains_url(https_url))
        self.assertFalse(draft_2019_09.contains_url(bad_url))

    def test_url_ignoring_fragments(self):
        no_fragment_url = "https://json-schema.org/draft/2020-12/schema"
        fragment_url = "https://json-schema.org/draft/2020-12/schema#"
        bad_url = "https://json-schema.org/draft/4444-44/schema"
        name = "Draft 2020-12"
        meta_id = "2012-12"
        draft_2019_09 = JsonDraft(name, meta_id, no_fragment_url)

        self.assertTrue(draft_2019_09.contains_url(no_fragment_url))
        self.assertTrue(draft_2019_09.contains_url(fragment_url))
        self.assertFalse(draft_2019_09.contains_url(bad_url))


if __name__ == "__main__":
    unittest.main()
