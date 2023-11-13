from setuptools import setup, find_packages
from datetime import datetime

# Generate a timestamp for the version
timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
version_postfix = '.dev.' + timestamp

# Read the requirements from requirements.txt file
with open('requirements.txt') as f:
    requirements = f.read().splitlines()

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name='json2spark_mapper',
    version='0.0.1' + version_postfix,
    description='Maps JSON schema types to Spark SQL types',
    long_description=long_description,
    long_description_content_type="text/markdown",
    url='https://github.com/vdweij/json-data-type-to-spark-sql-type-mapper'
    #author='',
    #author_email='',
    packages=['json2spark_mapper'],
    install_requires=requirements,  # Include the requirements from the file
)
