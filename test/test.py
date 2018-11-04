import datetime
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
        },
        debug=True,
    )

    con = psycopg2.connect('host=localhost dbname=jsonschema2db-test')
    translator.create_tables(con)
    translator.insert_items(con, [
        ('loan_file_abc123', {
            'Loan': {'Amount': 500000},
            'SubjectProperty': {'Address': {'City': 'New York', 'ZipCode': '12345', 'Latitude': 43}, 'Acreage': 42},
            'RealEstateOwned': {'1': {'Address': {'City': 'Brooklyn', 'ZipCode': '65432'}, 'RentalIncome': 1000},
                                '2': {'Address': {'City': 'Queens', 'ZipCode': '54321'}}},
        })
    ])
    translator.create_links(con)
    translator.analyze(con)

    assert list(query(con, 'select count(1) from schm.root')) == [(1,)]
    assert list(query(con, 'select count(1) from schm.basic_address')) == [(3,)]
    assert list(query(con, 'select loan_file_id, prefix, loan__amount, subject_property__acreage, subject_property__address__latitude, loan__abb_trlc from schm.root')) == \
        [('loan_file_abc123', '', 500000, 42.0, 43.0, None)]
    assert set(query(con, 'select loan_file_id, prefix, city, zip_code from schm.basic_address')) == \
        set([('loan_file_abc123', '/SubjectProperty/Address', 'New York', '12345'),
             ('loan_file_abc123', '/RealEstateOwned/1/Address', 'Brooklyn', '65432'),
             ('loan_file_abc123', '/RealEstateOwned/2/Address', 'Queens', '54321')])
    assert set(query(con, 'select loan_file_id, prefix, rental_income from schm.real_estate_owned')) == \
        set([('loan_file_abc123', '/RealEstateOwned/1', 1000),
             ('loan_file_abc123', '/RealEstateOwned/2', None)])
    assert set(query(con, 'select subject_property__address_id from schm.root union select address_id from schm.real_estate_owned')) == \
        set(query(con, 'select id from schm.basic_address'))
    assert set(query(con, 'select root_id from schm.real_estate_owned')) == \
        set(query(con, 'select id from schm.root'))


def test_pp_to_def():
    schema = json.load(open('test/test_pp_to_def.json'))
    translator = JSONSchemaToPostgres(schema, debug=True)
    con = psycopg2.connect('host=localhost dbname=jsonschema2db-test')
    translator.create_tables(con)
    translator.insert_items(con,
                            [(33, [(('aBunchOfDocuments', 'xyz', 'url'), 'http://baz.bar'),
                                   (('moreDocuments', 'abc', 'url'), 'https://banana'),
                                   (('moreDocuments', 'abc', 'url'), ['wrong-type']),
                                   (('moreDocuments', 'abc'), 'broken-value-ignore')])],
                            count=True)
    translator.create_links(con)
    translator.analyze(con)

    assert translator.failure_count == {('moreDocuments', 'abc'): 1, ('moreDocuments', 'abc', 'url'): 1}

    assert list(query(con, 'select count(1) from root')) == [(1,)]
    assert list(query(con, 'select count(1) from file')) == [(2,)]

    assert list(query(con, 'select id, prefix, item_id from root')) == [(1, '', 33)]
    assert list(query(con, 'select id, prefix, item_id, root_id from a_bunch_of_documents')) == \
        [(1, '/aBunchOfDocuments/xyz', 33, 1)]
    assert set(query(con, 'select prefix, url, item_id from file')) == \
        set([('/aBunchOfDocuments/xyz', 'http://baz.bar', 33),
             ('/moreDocuments/abc', 'https://banana', 33)])
    assert set(list(query(con, 'select file_id from a_bunch_of_documents')) +
               list(query(con, 'select file_id from more_documents'))) == set([(1,), (2,)])


def test_comments():
    schema = json.load(open('test/test_pp_to_def.json'))
    translator = JSONSchemaToPostgres(schema, debug=True)

    # A bit ugly to look at private members, but pulling comments out of postgres is a pain
    assert translator._table_comments == {'root': 'the root of everything',
                                          'file': 'this is a file',
                                          'a_bunch_of_documents': 'this is a bunch of documents'}
    assert translator._column_comments == {'file': {'url': 'the url of the file'}}


def test_time_types():
    schema = json.load(open('test/test_time_schema.json'))
    translator = JSONSchemaToPostgres(schema, debug=True)

    con = psycopg2.connect('host=localhost dbname=jsonschema2db-test')
    translator.create_tables(con)
    translator.insert_items(con, [
        (1, {'ts': datetime.datetime(2018, 2, 3, 12, 45, 56), 'd': datetime.date(2018, 7, 8)}),
        (2, {'ts': '2017-02-03T01:23:45Z', 'd': '2013-03-02'}),
    ])

    assert list(query(con, 'select id, d from root')) == \
        [(1, datetime.date(2018, 7, 8)), (2, datetime.date(2013, 3, 2))]
    assert list((id, ts.isoformat()) for id, ts in query(con, 'select id, ts from root')) == \
        [(1, '2018-02-03T12:45:56+00:00'), (2, '2017-02-03T01:23:45+00:00')]
