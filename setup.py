#!/usr/bin/env python

from os import path

from setuptools import setup, find_packages

this_directory = path.abspath(path.dirname(__file__))
with open(path.join(this_directory, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()

setup(
    name='singer-target-postgres',
    url='https://github.com/datamill-co/target-postgres',
    author='datamill',
    version="0.1.9",
    description='Singer.io target for loading data into postgres',
    long_description=long_description,
    long_description_content_type='text/markdown',
    classifiers=['Programming Language :: Python :: 3 :: Only'],
    py_modules=['target_postgres'],
    install_requires=[
        'arrow==0.13.0',
        'psycopg2==2.8.2',
        'psycopg2-binary==2.8.2',
        'singer-python==5.6.1'
    ],
    setup_requires=[
        "pytest-runner"
    ],
    tests_require=[
        "chance==0.110",
        "Faker==1.0.7",
        "pytest==4.5.0"
    ],
    entry_points='''
      [console_scripts]
      target-postgres=target_postgres:cli
    ''',
    packages=find_packages()
)
