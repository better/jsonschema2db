#!/usr/bin/env python

from setuptools import setup

setup(name='JSONSchema2DB',
      version='1.0',
      description='Generate database tables from JSON schema',
      author='Erik Bernhardsson',
      author_email='mail@erikbern.com',
      py_modules=['jsonschema2db'],
      install_requires=[
          'change_case==0.5.2',
          'iso8601==0.1.12',
          'psycopg2==2.7.2'
      ])
