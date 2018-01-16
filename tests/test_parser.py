from pg_to_brokers.parser import Parser


def test_parse():
    change_str = 'table public.companies: UPDATE: id[integer]:10 name[character varying]:\'Adidas2\'\'s adadisss\'\'\'\'\'\'\' statement[character varying]:\'you can do it\' logo[character varying]:\'\' sponsored_zip[json]:\'{"codes":["21111"]}\' website[character varying]:null external_employer_id[character varying]:\'\''
    location = '0/16E0478'
    transaction_id = 689
    changes = [(location, transaction_id, change_str)]
    parser = Parser()
    res = parser.parse(changes, [])[0]
    transaction_id = res['transaction_id']
    fields = res['fields']
    change_type = res['change_type']
    values = res['values']
    table_name = res['table_name']
    operation = res['operation']
    types = res['types']
    assert fields == [
        'id', 'name', 'statement', 'logo',
        'sponsored_zip', 'website', 'external_employer_id'
    ]
    assert change_type == 'table'
    assert values == [
        10, "Adidas2's adadisss'''", 'you can do it',
        '', '{"codes":["21111"]}', None, ''
    ]
    assert table_name == 'public.companies'
    assert operation == 'UPDATE'
    assert types == [
        'integer', 'character varying', 'character varying',
        'character varying', 'json', 'character varying',
        'character varying'
    ]
