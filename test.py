import json
import psycopg2

from datalake_etl import loan_file_fact_flat, util


def test_lff():
    con = psycopg2.connect('host=localhost dbname=datalake-etl-test')
    with con.cursor() as cursor:
        cursor.execute('create table loan_file_fact (id int, loan_file_id int, path text, value jsonb, fact_type text, created_at timestamptz)')
        cursor.execute('insert into loan_file_fact values (1, 1000000000, \'/Loan/Amount\', \'500000\'::jsonb, \'Value\', now())')
        cursor.execute('insert into loan_file_fact values (1, 1000000000, \'/SubjectProperty/Address/City\', \'"New York"\'::jsonb, \'Value\', now())')

    assert list(util.query(con, 'select count(1) from loan_file_fact')) == [(2,)]

    loan_file_fact_flat.run(con, is_test=True)

    assert list(util.query(con, 'select count(1) from derived_lff.root')) == [(1,)]
    assert list(util.query(con, 'select loan__amount from derived_lff.root')) == [(500000,)]
    assert list(util.query(con, 'select city from derived_lff.basic_address')) == [('New York',)]
    assert list(util.query(con, 'select subject_property__address_id from derived_lff.root')) \
        == list(util.query(con, 'select id from derived_lff.basic_address'))
    assert list(util.query(con, 'select prefix from derived_lff.basic_address')) == [('/SubjectProperty/Address',)]
    assert list(util.query(con, 'select prefix from derived_lff.root')) == [('',)]
