import json
import psycopg2

from jsonschema2db import JSONSchemaToPostgres


def test_lff():
    con = psycopg2.connect('host=localhost dbname=jsonschema2db-test')

    
    assert list(util.query(con, 'select count(1) from derived_lff.root')) == [(1,)]
    assert list(util.query(con, 'select loan__amount from derived_lff.root')) == [(500000,)]
    assert list(util.query(con, 'select city from derived_lff.basic_address')) == [('New York',)]
    assert list(util.query(con, 'select subject_property__address_id from derived_lff.root')) \
        == list(util.query(con, 'select id from derived_lff.basic_address'))
    assert list(util.query(con, 'select prefix from derived_lff.basic_address')) == [('/SubjectProperty/Address',)]
    assert list(util.query(con, 'select prefix from derived_lff.root')) == [('',)]
