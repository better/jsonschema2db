import json
import psycopg2

from jsonschema2db import JSONSchemaToPostgres


def query(con, q):
    cur = con.cursor()
    cur.execute(q)
    return cur.fetchall()


def test_lff():
    schema = json.load(open('test/test_schema.json'))
    translator = JSONSchemaToPostgres(schema,
                                      postgres_schema='schm',
                                      item_col_name='loan_file_id')

    con = psycopg2.connect('host=localhost dbname=jsonschema2db-test')

    translator.create_tables(con)
    translator.insert_items(con, {1000000000: {'Loan': {'Amount': 500000}, 'SubjectProperty': {'Address': {'City': 'New York'}}}})
    translator.create_links(con)

    assert list(query(con, 'select count(1) from schm.root')) == [(1,)]
    assert list(query(con, 'select loan__amount from schm.root')) == [(500000,)]
    assert list(query(con, 'select city from schm.basic_address')) == [('New York',)]
    assert list(query(con, 'select subject_property__address_id from schm.root')) \
        == list(query(con, 'select id from schm.basic_address'))
    assert list(query(con, 'select prefix from schm.basic_address')) == [('/SubjectProperty/Address',)]
    assert list(query(con, 'select prefix from schm.root')) == [('',)]
