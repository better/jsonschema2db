import json
import psycopg2

from jsonschema2db import JSONSchemaToPostgres


def query(con, q):
    cur = con.cursor()
    cur.execute(q)
    return cur.fetchall()


def test_lff():
    schema = json.load(open('test/test_schema.json'))
    translator = JSONSchemaToPostgres(
        schema,
        postgres_schema='schm',
        item_col_name='loan_file_id',
        item_col_type='string',
        abbreviations={
            'AbbreviateThisReallyLongColumn': 'AbbTRLC',
        }
    )

    con = psycopg2.connect('host=localhost dbname=jsonschema2db-test')
    con.autocommit = True

    translator.create_tables(con)
    translator.insert_items(con, {
        'loan_file_abc123': {
            'Loan': {'Amount': 500000},
            'SubjectProperty': {'Address': {'City': 'New York', 'ZipCode': '12345', 'Latitude': 43}, 'Acreage': 42},
            'RealEstateOwned': {'1': {'Address': {'City': 'Brooklyn', 'ZipCode': '65432'}, "RentalIncome": 1000}}
        }
    })
    translator.create_links(con)
    translator.analyze(con)

    assert list(query(con, 'select count(1) from schm.root')) == [(1,)]
    assert list(query(con, 'select count(1) from schm.basic_address')) == [(2,)]
    assert list(query(con, 'select loan_file_id, prefix, loan__amount, subject_property__acreage, subject_property__address__latitude, loan__abb_trlc from schm.root')) == \
        [('loan_file_abc123', '', 500000, 42.0, 43.0, None)]
    assert set(query(con, 'select loan_file_id, prefix, city, zip_code from schm.basic_address')) == \
        set([('loan_file_abc123', '/SubjectProperty/Address', 'New York', '12345'),
             ('loan_file_abc123', '/RealEstateOwned/1/Address', 'Brooklyn', '65432')])
    assert set(query(con, 'select loan_file_id, prefix, rental_income from schm.real_estate_owned')) == \
        set([('loan_file_abc123', '/RealEstateOwned/1', 1000)])
    assert set(query(con, 'select subject_property__address_id from schm.root union select address_id from schm.real_estate_owned')) == \
        set(query(con, 'select id from schm.basic_address'))
    assert query(con, 'select root_id from schm.real_estate_owned') == \
        query(con, 'select id from schm.root')
