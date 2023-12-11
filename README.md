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

## How to use

### Install package
First make sure you install the module into your environment. There are various options assuming you have a Python 3.* environment set up:

#### Install from PyPI
Not yet available. Working on it.

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

```python
with open("path-to-your-schema.json") as schema_file:
    json_schema = json.load(schema_file)
struct_type = from_json_to_spark(json_schema)
```

### Troubleshooting
Nothing here yet as this is pretty straight forward, right?!

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

### More

See for more [development information](docs/development.md).
