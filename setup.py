#!/usr/bin/env python

from setuptools import setup
from sphinx.setup_command import BuildDoc

long_description = '''
For more information, see
`the package documentation <https://better.engineering/jsonschema2db>`_
or
`the Github project page <https://github.com/better/jsonschema2db>`_.
'''

setup(name='JSONSchema2DB',
      version='1.0.2',
      description='Generate database tables from JSON schema',
      long_description=long_description,
      url='https://better.engineering/convoys',
      license='MIT',
      author='Erik Bernhardsson',
      author_email='erikbern@better.com',
      py_modules=['jsonschema2db'],
      install_requires=[
          'change_case==0.5.2',
          'iso8601==0.1.12',
          'psycopg2==2.7.2'
      ],

      # Sphinx-specific setup
      cmdclass={'build_sphinx': BuildDoc},
      command_options={
        'build_sphinx': {
            'source_dir': ('setup.py', 'docs'),
            'build_dir': ('setup.py', 'docs/_build')}},
)
