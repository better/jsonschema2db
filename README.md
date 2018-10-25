[![Travis status](https://img.shields.io/travis/better/jsonschema2db/master.svg?style=flat)](https://travis-ci.org/better/jsonschema2db)

JSON Schema ➣ Database
---

We use [JSON Schema](http://json-schema.org/) pretty extensively at [Better](https://better.com) to store complex data. Unfortunately the data is hard to query from SQL. To facilitate querying, this library converts objects stored in a JSON schema into flat Postgres tables with proper types.

For instance, Better uses it to generate 50+ tables automatically, with millions of rows, from a very complex JSON schema that is 7000+ lines long.

Postgres and Redshift are supported, although the latter is somewhat experimental still.

Installation
---

The easiest way to install this is (probably) to run `pip install -e git://github.com/better/jsonschema2db#egg=jsonschema2db`

Documentation
---

See [full documentation](https://better.engineering/jsonschema2db/) for more info about how to use jsonschema2db!

Other
---

* The code is tested with a full integration test running a Postgres server inside Docker. To run tests, run `docker build -t jsonschema2db . && docker run jsonschema2db`
* Pull requests are very welcome.
* This repo uses the [MIT license](https://github.com/better/jsonschema2db/blob/master/LICENSE).
