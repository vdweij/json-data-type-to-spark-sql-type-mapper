[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
[![Checked with mypy](https://www.mypy-lang.org/static/mypy_badge.svg)](https://mypy-lang.org/)
[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/charliermarsh/ruff/main/assets/badge/v0.json)](https://github.com/charliermarsh/ruff)

# json-data-type-to-spark-sql-type-mapper
A python mapper that converts JSON data types to a Spark SQL type

## Introduction
Spark has built in support for converting [Avro data types](https://avro.apache.org/docs/1.11.1/specification/) into Spark SQL types, but lacks similar functionality with regard to JSON data types. Instead Spark has logic that can [infer types](https://spark.apache.org/docs/latest/sql-data-sources-json.html) from sample JSON documents. At first glance this might appear sufficient, however at closer inspection some disadvantages surface. For instance:

 - It is impossible to define a StructField as optional. Every subsequent JSON document that is processed needs to supply all the fields that were present in the initial JSON document.
 - Numeric values, both JSON integer and number, should be converted to the largest Spark type because ranges are unknown. This could lead additional storage requirements, although minimal in modern systems. When using a JSON schema that specifies ranges the right Spark type can be selected.
 - JSON arrays can be a pain. In most positive scenario they act as a `list` containing a single type, but they could also be used to define `tuple` structures with mandatory types and additional elements of any type.

This package provides a mapping function that can be used similar to how avro schemas are used whilst keeping all relevant details to create a StructType with optimal StructFields. See the [supported types](docs/types.md).

> **Note:** Looking for feedback from users that take this for a spin to work with real life

## How to use

### Install package
First make sure you install the module into your environment. There are various options assuming you have a Python 3.10 (or higher) environment set up:

#### Install from PyPI

```bash
pip install json2spark-mapper
```

#### Install from TestPyPI

```bash
pip install --index-url https://test.pypi.org/simple/ --extra-index-url https://pypi.org/simple/  json2spark-mapper
```
Note: because the required package pyspark is not available in TestPyPI the `extra-index-url` is needed.

#### From source
- checkout the project
- navigate to the root directory
- simply issue `pip install .`

```bash
git clone https://github.com/vdweij/json-data-type-to-spark-sql-type-mapper.git
cd json-data-type-to-spark-sql-type-mapper
pip install .
```

### Import module

In order to make the mapper function `from_json_to_spark` available in your Python file, use the following import statement:

```python
from json2spark_mapper.schema_mapper import from_json_to_spark
```

### Call mapping function

#### Simplest form

```python
with open("path-to-your-schema.json") as schema_file:
    json_schema = json.load(schema_file)
struct_type = from_json_to_spark(json_schema)
```

#### Setting default json draft version

Per default the most recent json draft version is used in case a schema lacks a `@schema` definition. The function uses
`JSON_DRAFTS.get_latest() from module json_schema_drafts.drafts` to specify the default. This can be overridden by explictily setting
it to a desired draft version, like so:

```python
json_schema = json.load(schema_file, default_json_draft=JSON_DRAFTS.draft_2019_09)
```

Don't forget to import `JSON_DRAFT`!
```python
from json2spark_mapper.schema_mapper import JSON_DRAFTS
```

#### Forcing a json draft version

The default behaviour is to use the draft version specified in the schema or when it is abcent to use the `default_json_draft` version.
This can be overridden by specifying the `force_json_draft` property, like so:

```python
json_schema = json.load(schema_file, force_json_draft=JSON_DRAFTS.draft_2019_09)
```

The version specified via `force_json_draft` takes precedence regardless of what is specified in the schema or by the default `default_json_draft`.

Don't forget to import `JSON_DRAFT`!
```python
from json2spark_mapper.schema_mapper import JSON_DRAFTS
```

### Defining own Resolvers

It is possible to create your own readers for various Json types. This can be done by creating an implementation of either the abstract Root `TypeResolver` or via one of its abstract type resolvers.

This resolver should then be added to the appropiate `ResolverRegistry` that needs to be created for each Json draft version that is needed.

This registry could then in turn be added to the `ResolverAwareReader` and needs to be assigned to `READER` of `schame_mapper`.

### Troubleshooting

## ERROR: No matching distribution found for json2spark-mapper==[_some-version_]

This library is packaged with the explict instruction `python_requires=">=3.10"` which means pip will not be able to find this
library when tried to be installed from older Python versions.

In case of **Databricks** make sure to use a runtime higher than or equal to 13.2 that comes with Python 3.10.2.

## TypeError: unsupported operand type(s) for |: 'type' and 'NoneType'

Python 3.10 introduced [PEP 604 – Allow writing union types as X | Y](https://peps.python.org/pep-0604/) and this form of type
hints are used throughout the code. Make sure your project used Python 3.10 or higher.

In case of **Databricks** make sure to use a runtime higher than or equal to 13.2 that comes with Python 3.10.2.

### Issues
Please check existing issues before creating a new one.

## Development

For development, install the `[dev]` dependencies of the package.
This will also install [pre-commit](https://pre-commit.com/).

Install pre-commit so that it automatically runs whenever you create a
new commit, to ensure code quality before pushing.

```bash
pip install .[dev]
pre-commit install
```

In order to run unittest locally also install the `[test]` dependencies.

```bash
pip install .[test]
```

### Pre-commit

Pre-commit is configured to lint and autofix all files with standard.

Python code linting is done with [black](https://pypi.org/project/black/) and [ruff](https://pypi.org/project/ruff/) with a selection
of ruff plugins. Details are in `.pre-commit-config.yaml`.

### Release Notes

See for [release notes](docs/releases.md).

### More

See for more [development information](docs/development.md).
