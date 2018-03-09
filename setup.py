#!/usr/bin/env python

from setuptools import setup, find_packages

setup(
    name='target-postgres',
    version="0.0.1",
    description='Singer.io target for loading data into postgres',
    classifiers=['Programming Language :: Python :: 3 :: Only'],
    py_modules=['target_postgres'],
    install_requires=[
        'psycopg2==2.7.4',
        'psycopg2-binary==2.7.4',
        'singer-python==5.0.12'
    ],
    entry_points='''
      [console_scripts]
      target-postgres=target_postgres:main
    ''',
    packages=find_packages()
)
