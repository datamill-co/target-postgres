#!/usr/bin/env python

from setuptools import setup, find_packages

setup(
    name='target-sql',
    version="0.0.1",
    description='Singer.io targets for loading data into SQL databases',
    classifiers=['Programming Language :: Python :: 3 :: Only'],
    py_modules=['target_postgres'],
    install_requires=[
        'jsonschema==2.6.0',
        'psycopg2==2.7.4',
        'psycopg2-binary==2.7.4',
        'singer-python==5.0.12'
    ],
    entry_points='''
      [console_scripts]
      target-postgres=target_sql:target_postgres_main
      target-redshift=target_sql:target_redshift_main
    ''',
    packages=find_packages()
)
