import pytest
from singer_sdk.helpers._flattening import flatten_schema
from target_parquet.utils.parquet import flatten_schema_to_pyarrow_schema, _field_type_to_pyarrow_field, \
    create_pyarrow_table, concat_tables
import pyarrow as pa
import pandas as pd


def test_flatten_schema_to_pyarrow_schema():
    schema = {"type": "object",
              "properties": {"str": {"type": ["null", "string"]},
                             "int": {"type": ["null", "integer"]},
                             "decimal": {"type": ["null", "number"]},
                             "decimal2": {"type": ["null", "number"]},
                             "date": {"type": ["null", "string"], "format": "date-time"},
                             "datetime": {"type": ["null", "string"], "format": "date-time"},
                             "boolean": {"type": ["null", "boolean"]}}}
    flatten_schema_result = flatten_schema(schema, max_level=20)
    pyarrow_schema = flatten_schema_to_pyarrow_schema(flatten_schema_result)
    expected_pyarrow_schema = pa.schema([
        pa.field('str', pa.string()),
        pa.field('int', pa.int64()),
        pa.field('decimal', pa.float64()),
        pa.field('decimal2', pa.float64()),
        pa.field('date', pa.string()),
        pa.field('datetime', pa.string()),
        pa.field('boolean', pa.bool_())])
    assert pyarrow_schema == expected_pyarrow_schema


@pytest.mark.parametrize(
    "field_name, input_types, expected_result",
    [
        pytest.param(
            "example_field",
            {"type": "string"},
            pa.field("example_field", pa.string(), True),
            id="valid_input",
        ),
        pytest.param(
            "example_field_anyof",
            {"anyOf": [{"type": "integer"}, {"type": "string"}]},
            pa.field("example_field_anyof", pa.int64(), False),
            id="anyof_input",
        ),
        pytest.param(
            "unknown_type",
            {"type": "unknown_type"},
            pa.field("unknown_type", pa.string(), True),
            id="unknown_type"
        ),
    ],
)
def test_field_type_to_pyarrow_field(field_name, input_types, expected_result):
    result = _field_type_to_pyarrow_field(field_name, input_types, ['example_field_anyof'])
    assert result == expected_result


def test_create_pyarrow_table():
    schema = pa.schema([
        ("id", pa.int64()),
        ("name", pa.string()),
        ("age", pa.int64()),
    ])
    data = [
        {"id": 1, "name": "Alice", "age": 25},
        {"id": 2, "name": "Bob"},
        {"id": 3, "age": 22},
    ]
    expected_table = pd.DataFrame(data)
    result_table = create_pyarrow_table(data, schema)

    # Check if the result has the expected schema
    assert result_table.schema.equals(schema)
    # Check if the result has the expected number of rows
    assert len(result_table) == len(data)
    # Check if the result has the expected data
    assert result_table.to_pandas().equals(expected_table)


def test_concat_tables():
    # Define the initial PyArrow schema and table
    initial_schema = pa.schema([
        ("id", pa.int64()),
        ("name", pa.string()),
        ("age", pa.int64()),
    ])
    sample_data = [
        {"id": 1, "name": "Alice", "age": 25},
        {"id": 2, "name": "Bob", "age": 30},
        {"id": 3, "name": "Charlie", "age": 22},
    ]

    initial_table = create_pyarrow_table(sample_data, initial_schema)

    # Call concat_tables with sample data
    result_table = concat_tables(sample_data, initial_table, initial_schema)

    # Create the expected PyArrow table using create_pyarrow_table
    expected_table = create_pyarrow_table(sample_data * 2, initial_schema)

    # Check if the resulting PyArrow table is equal to the expected table
    assert result_table.equals(expected_table)
