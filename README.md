[![Travis status](https://img.shields.io/travis/better/jsonschema2db/master.svg?style=flat)](https://travis-ci.org/better/jsonschema2db)

JSON Schema ➣ Database
---

We use [JSON Schema](http://json-schema.org/) pretty extensively at [Better](https://better.com) to store complex data. Unfortunately the data is hard to query from SQL. To facilitate querying, this library converts objects stored in a JSON schema into flat Postgres tables with proper types.

For instance, Better uses it to generate 50+ tables automatically, with millions of rows, from a very complex JSON schema that is 7000+ lines long.

Only Postgres is supported at the moment, although it should be fairly simple to add other databases.

Example
---

Let's say you have this schema: [test_schema.json](https://github.com/better/jsonschema2db/blob/master/test/test_schema.json). Basically:

1. There's a shared definition for an `basicAddress` definition that has the normal address fields: state, zip, etc
2. There's a definition `address` that extends `basicAddress` and adds latitude and longitude
3. Each "loan" (we are in the mortgage industry) tracks a loan amount, a bunch of info (including address) for the subject property (the property that the loan is against), and a list of other properties owned by the borrower (each of which has an address and a rental income)

jsonschema2db creates the following tables automatically:

```
create table "schm"."root" (
       id serial primary key,
       "loan_file_id" int not null,
       "prefix" text not null,
       "loan__amount" float,
       "subject_property__acreage" float,
       "subject_property__address__latitude" float,
       "subject_property__address__longitude" float,
       "subject_property__address_id" integer,
       unique ("loan_file_id", "prefix")
)

create table "schm"."basic_address" (
       id serial primary key,
       "loan_file_id" int not null,
       "prefix" text not null,
       "city" text,
       "root_id" integer,
       "state" text,
       "street" text,
       "zip_code" text,
       unique ("loan_file_id", "prefix")
)

create table "schm"."real_estate_owned" (
       id serial primary key,
       "loan_file_id" int not null,
       "prefix" text not null,
       "address_id" integer,
       "rental_income" float,
       "root_id" integer,
       unique ("loan_file_id", "prefix")
)
```

As you can see, we end up with three tables, each containing a flat structure of scalar values, with correct types. jsonschema2db also converts camel case into snake case since that's the Postgres convention.

jsonschema2db also handles inserts into these tables by transforming them into a flattened form. On top of that, a number of foreign keys will be created and links between the tables.

The rule for when to create a separate table is that either:

1. It's a shared definition that is an object (with links from the parent to the child)
2. Any object with `patternProperties` will have its children in a separate table (with links back to the parent, if the link is unique)

Other
---

* The code is tested with a full integration test running a Postgres server inside Docker. To run tests, run `docker build -t jsonschema2db . && docker run jsonschema2db`
* Pull requests are very welcome.
* This repo uses the [MIT license](https://github.com/better/jsonschema2db/blob/master/LICENSE).
