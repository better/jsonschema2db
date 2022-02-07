#!/usr/bin/env python

from setuptools import setup

long_description = '''
For more information, see
`the package documentation <https://better.engineering/jsonschema2db>`_
or
`the Github project page <https://github.com/better/jsonschema2db>`_.
'''

setup(name='JSONSchema2DB',
      version='1.0.1',
      description='Generate database tables from JSON schema',
      long_description=long_description,
      url='https://better.engineering/jsonschema2db',
      license='MIT',
      author='Erik Bernhardsson',
      author_email='erikbern@better.com',
      py_modules=['jsonschema2db'],
      install_requires=[
          'change_case>=0.5.2',
          'iso8601>=0.1.12',
          'psycopg2-binary>=2.7.2'
      ])
