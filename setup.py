from setuptools import setup, find_packages

# Read the requirements from requirements.txt file
with open('requirements.txt') as f:
    requirements = f.read().splitlines()

setup(
    name='json2spark_mapper',
    version='0.0.1',
    description='Maps JSON schema types to Spark SQL types',
    #author='',
    #author_email='',
    packages=['json2spark_mapper'],
    install_requires=requirements,  # Include the requirements from the file
)
