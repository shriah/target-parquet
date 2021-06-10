import pytest

from target_parquet.helpers import flatten, flatten_schema


def test_flatten():
    in_dict = {
        "key_1": 1,
        "key_2": {"key_3": 2, "key_4": {"key_5": 3, "key_6": ["10", "11"]}},
    }
    expected = {
        "key_1": 1,
        "key_2__key_3": 2,
        "key_2__key_4__key_5": 3,
        "key_2__key_4__key_6": "['10', '11']",
    }

    output = flatten(in_dict)
    assert output == expected

def test_flatten_schema():
    in_dict = {
        'key_1': {'type': ['null', 'integer']},
        'key_2': {
            'type': ['null', 'object'],
            'properties': {
                'key_3': {'type': ['null', 'string']},
                'key_4': {
                    'type': ['null', 'object'],
                    'properties': {
                        'key_5' : {'type': ['null', 'integer']},
                        'key_6' : {
                            'type': ['null', 'array'],
                            'items': {
                                'type': ['null', 'object'],
                                'properties': {
                                    'key_7': {'type': ['null', 'number']},
                                    'key_8': {'type': ['null', 'string']}
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    expected = [
             'key_1',
             'key_2__key_3',
             'key_2__key_4__key_5',
             'key_2__key_4__key_6'
    ]

    output = flatten_schema(in_dict)
    assert output == expected
