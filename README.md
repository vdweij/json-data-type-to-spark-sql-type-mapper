# json-data-type-to-spark-sql-type-mapper
A python mapper that converts JSON data types to a Spark SQL type

## Introduction
Spark has built in support for converting [Avro data types](https://avro.apache.org/docs/1.11.1/specification/) into Spark SQL types, but lacks similar functionality in regards to JSON data types. Instead Spark has logic that can [infer types](https://spark.apache.org/docs/latest/sql-data-sources-json.html) from sample JSON documents. At first glance this might appear sufficient, however at closer inspection some disadvantages surface. For instance:

 - It is impossible to define a StructField as optional. Every subsequent JSON document that is processed needs to supply all the fields that were present in the initial JSON document.
 - Numberic values (both JSON integer and number) should be converted to the largest Spark type because ranges are unknown. This could lead additional storage requirements (although minimal in modern systems). When using a JSON schema that specifies ranges the right Spark type can be selected.

This package provides a mapping function that can be used similar to how avro schemas are used whilst keeping all relevant details to create a StructType with optimal StructFields. See the [supported types](docs/types.md).

## How to use

### Install package
First make sure you install the module into your environment. There are various options assuming you have a Python (3) environment set up:

#### Install from PyPI
Not yet available. Working on it.

#### Install from TestPyPI

```bash
pip install --index-url https://test.pypi.org/simple/ --extra-index-url https://pypi.org/simple/  json2spark-mapper
```
Note: because de required package pyspark is not available in TestPyPI the `extra-index-url` is needed.

#### Install from source code
- checkout the project
- navigate to the root directory
- simply issue `pip install .`

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

### Trouble shooting
Nothing here yet as this is pretty straight forward, right?!

### Issues
Please check existing issues before creating a new one.