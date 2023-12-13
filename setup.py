from datetime import datetime

from setuptools import setup

# Generate a timestamp for the version
timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
version_postfix = ".dev." + timestamp

# Read the requirements from requirements.txt file
with open("requirements/core.txt") as f:
    core_requirements = f.read().splitlines()

with open("requirements/dev.txt") as f:
    dev_requirements = f.read().splitlines()

with open("README.md", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name="json2spark_mapper",
    version="0.0.2" + version_postfix,
    description="Maps JSON schema types to Spark SQL types",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/vdweij/json-data-type-to-spark-sql-type-mapper",
    author="AvdW",
    packages=["json2spark_mapper"],
    install_requires=core_requirements,  # Include the requirements from the file
    extras_require={"dev": dev_requirements},
)
