from setuptools import setup, find_packages

from codecs import open
from os import path

here = path.abspath(path.dirname(__file__))

# Get the long description from the README file
with open(path.join(here, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()

setup(
    name='Spark_Testing',
    version='0.1',
    description='Unit test lib for spark dataframes',
    long_description=long_description,
    url='https://github.com/xebia-france/pyspark-test',
    author='Sylvain Lequeux',
    author_email='slequeux@xebia.fr',
    classifiers=[
        'Development Status :: 1 - Planning',
        'Intended Audience :: Developers',
        'Topic :: Software Development :: Testing'
    ],
    keywords='spark pyspark test hamcrest matcher',
    packages=find_packages(exclude=['tests*']),
    requires=['pyhamcrest'],
    tests_requires=['pytest', 'unittest2', 'pyhamcrest'],
)
