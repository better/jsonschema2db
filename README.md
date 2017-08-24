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

As you can see, we end up with three tables, each containing a flat structure of scalar values, with correct types. jsonschema2db also converts camel case into snake case since that's the Postgres convention. Unfortunately, Postgres limits column names to 63 characters. If you have longer column names, provide a list of abbreviations using the `abbreviations` parameter to the constructor.

jsonschema2db also handles inserts into these tables by transforming them into a flattened form. On top of that, a number of foreign keys will be created and links between the tables.

The rule for when to create a separate table is that either:

1. It's a shared definition that is an object (with links from the parent to the child)
2. Any object with `patternProperties` will have its children in a separate table (with links back to the parent, if the link is unique)

Inserting data
---

Let's say we have the following object with key `1000000000`:

```
{1000000000: {'Loan': {'Amount': 500000},
              'RealEstateOwned': {'1': {'Address': {'City': 'Brooklyn',
                                                    'ZipCode': '65432'},
                                        'RentalIncome': 1000}},
              'SubjectProperty': {'Acreage': 42,
                                  'Address': {'City': 'New York',
                                              'Latitude': 43,
                                              'ZipCode': '12345'}}}}
```

If we insert this into the database using jsonschema2db, it will create the following rows:

```
jsonschema2db-test=# select * from schm.root;
-[ RECORD 1 ]------------------------+-----------
id                                   | 1
loan_file_id                         | 1000000000
prefix                               |
loan__amount                         | 500000
subject_property__acreage            | 42
subject_property__address__latitude  | 43
subject_property__address__longitude |
subject_property__address_id         | 1

jsonschema2db-test=# select * from schm.basic_address;
-[ RECORD 1 ]+---------------------------
id           | 2
loan_file_id | 1000000000
prefix       | /RealEstateOwned/1/Address
city         | Brooklyn
root_id      | 1
state        |
street       |
zip_code     | 65432
-[ RECORD 2 ]+---------------------------
id           | 1
loan_file_id | 1000000000
prefix       | /SubjectProperty/Address
city         | New York
root_id      | 1
state        |
street       |
zip_code     | 12345

jsonschema2db-test=# select * from schm.real_estate_owned;
-[ RECORD 1 ]-+-------------------
id            | 1
loan_file_id  | 1000000000
prefix        | /RealEstateOwned/1
address_id    | 2
rental_income | 1000
root_id       | 1
```

Usage
---

```python
class JSONSchemaToPostgres:
    def __init__(self,
                 schema,  # The JSON schema, as a native Python dict
                 postgres_schema=None,  # optimally a string denoting a postgres schema (namespace) under which all tables will be created
                 item_col_name='item_id',  # the name of the main object key
                 item_col_type='integer',  # type of the main object key (uses the type identifiers from JSON Schema)
                 prefix_col_name='prefix',  # postgres column name identifying the subpaths in the object
                 abbreviations={}):  # a string to string mapping containing replacements applied to each part of the path

    def create_tables(self,
                      con): # psycopg2 connection object

    def insert_items(self,
                     con,
                     items,  # dict of key to objects using the JSON schema
                     failure_count={}):  # Count failures by path (broken properties etc)

    def create_links(self,
                     con): # psycopg2 connection object

    def analyze(self,
                con): # psycopg2 connection object
```

Typically you want to instantiate a `JSONSchemaToPostgres` object, and run `create_tables` to create all the tables. After that, insert all data. Once you're done inserting, run `create_links` to populate all references properly and add foreign keys between tables. Optionally you can run `analyze` finally which optimizes the tables.

Other
---

* The code is tested with a full integration test running a Postgres server inside Docker. To run tests, run `docker build -t jsonschema2db . && docker run jsonschema2db`
* Pull requests are very welcome.
* This repo uses the [MIT license](https://github.com/better/jsonschema2db/blob/master/LICENSE).
