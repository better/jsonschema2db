[![Travis status](https://img.shields.io/travis/better/jsonschema2db/master.svg?style=flat)](https://travis-ci.org/better/jsonschema2db)

JSON Schema ➣ Database
---

We use [JSON Schema](http://json-schema.org/) pretty extensively at [Better](https://better.com) to store complex data. Unfortunately the data is hard to query from SQL. To facilitate querying, this library converts objects stored in a JSON schema into flat Postgres tables with proper types.

Only Postgres is supported at the moment, although it should be fairly simple to add other databases.

Example
---

TO DO – see [test.py](https://github.com/better/jsonschema2db/blob/master/test/test.py) for now.

Other
---

* The code is tested with a full integration test running a Postgres server inside Docker.
* Pull requests are very welcome.
* This repo uses the [MIT license](https://github.com/better/jsonschema2db/blob/master/LICENSE).
