import os
import tempfile
import pandas as pd
import pytest
import io
from datetime import datetime
from decimal import Decimal
import pyarrow as pa
from pyarrow.parquet import ParquetFile
from pandas.testing import assert_frame_equal
import glob
from target_parquet import persist_messages, create_dataframe


@pytest.fixture
def expected_df_1():
    return pa.table(
        {
            "str": ["value1", "value2", "value3"],
            "int": [1, None, 3],
            "decimal": [Decimal("0.1"), Decimal("0.2"), Decimal("0.3")],
            "date": ["2021-06-11", "2021-06-12", "2021-06-13"],
            "datetime": [
                "2021-06-11T00:00:00.000000Z",
                "2021-06-12T00:00:00.000000Z",
                "2021-06-13T00:00:00.000000Z",
            ],
            "boolean": [True, True, False],
        }
    ).to_pandas()

@pytest.fixture
def expected_df_2():
    # date field have all values null
    return pa.table(
        {
            "str": ["value1", "value2", "value3"],
            "int": [1, None, 3],
            "decimal": [Decimal("0.1"), Decimal("0.2"), Decimal("0.3")],
            "datetime": [
                "2021-06-11T00:00:00.000000Z",
                "2021-06-12T00:00:00.000000Z",
                "2021-06-13T00:00:00.000000Z",
            ],
            "boolean": [True, True, False],
        }
    ).to_pandas()


@pytest.fixture
def expected_df_3():
    return pd.DataFrame(
        [
            {'field1__field2__field3': 'test_field3',
             'field1__field2__field4': 'test_field4',
             'field2__field3': None,
             'field2__field4': None,
             'field2__field5': None,
             'field6': None,
             }
        ]
    )


@pytest.fixture
def input_messages_1():
    return """\
{"type": "SCHEMA","stream": "test","schema": {"type": "object","properties": {"str": {"type": ["null", "string"]},"int": {"type": ["null", "integer"]},"decimal": {"type": ["null", "number"]},"date": {"type": ["null", "string"], "format": "date-time"},"datetime": {"type": ["null", "string"], "format": "date-time"},"boolean": {"type": ["null", "boolean"]}}}, "key_properties": ["str"]}
{"type": "RECORD", "stream": "test", "record": {"str": "value1","int": 1,"decimal": 0.1,"date": "2021-06-11","datetime": "2021-06-11T00:00:00.000000Z","boolean": true}}
{"type": "STATE", "value": {"datetime": "2020-10-19"}}
{"type": "SCHEMA","stream": "test","schema": {"type": "object","properties": {"str": {"type": ["null", "string"]},"int": {"type": ["null", "integer"]},"decimal": {"type": ["null", "number"]},"date": {"type": ["null", "string"], "format": "date-time"},"datetime": {"type": ["null", "string"], "format": "date-time"},"boolean": {"type": ["null", "boolean"]}}}, "key_properties": ["str"]}
{"type": "RECORD", "stream": "test", "record": {"str": "value2","decimal": 0.2,"date": "2021-06-12","datetime": "2021-06-12T00:00:00.000000Z","boolean": true}}
{"type": "RECORD", "stream": "test", "record": {"str": "value3","int": 3,"decimal": 0.3,"date": "2021-06-13","datetime": "2021-06-13T00:00:00.000000Z","boolean": false}}
{"type": "STATE", "value": {"datetime": "2020-10-19"}}
"""


@pytest.fixture
def input_messages_1_reorder():
    return """\
{"type": "RECORD", "stream": "test", "record": {"str": "value1","int": 1,"decimal": 0.1,"date": "2021-06-11","datetime": "2021-06-11T00:00:00.000000Z","boolean": true}}
{"type": "SCHEMA","stream": "test","schema": {"type": "object","properties": {"str": {"type": ["null", "string"]},"int": {"type": ["null", "integer"]},"decimal": {"type": ["null", "number"]},"date": {"type": ["null", "string"], "format": "date-time"},"datetime": {"type": ["null", "string"], "format": "date-time"},"boolean": {"type": ["null", "boolean"]}}}, "key_properties": ["str"]}
{"type": "STATE", "value": {"datetime": "2020-10-19"}}
{"type": "SCHEMA","stream": "test","schema": {"type": "object","properties": {"str": {"type": ["null", "string"]},"int": {"type": ["null", "integer"]},"decimal": {"type": ["null", "number"]},"date": {"type": ["null", "string"], "format": "date-time"},"datetime": {"type": ["null", "string"], "format": "date-time"},"boolean": {"type": ["null", "boolean"]}}}, "key_properties": ["str"]}
{"type": "RECORD", "stream": "test", "record": {"str": "value2","decimal": 0.2,"date": "2021-06-12","datetime": "2021-06-12T00:00:00.000000Z","boolean": true}}
{"type": "RECORD", "stream": "test", "record": {"str": "value3","int": 3,"decimal": 0.3,"date": "2021-06-13","datetime": "2021-06-13T00:00:00.000000Z","boolean": false}}
{"type": "STATE", "value": {"datetime": "2020-10-19"}}
"""


@pytest.fixture
def input_messages_2_null_col_with_different_datatype():
    return """\
{"type": "SCHEMA","stream": "test","schema": {"type": "object","properties": {"str": {"type": ["null", "string"]},"int": {"type": ["null", "integer"]},"decimal": {"type": ["null", "number"]},"decimal2": {"type": ["null", "number"]},"date": {"type": ["null", "string"], "format": "date-time"},"datetime": {"type": ["null", "string"], "format": "date-time"},"boolean": {"type": ["null", "boolean"]}}}, "key_properties": ["str"]}
{"type": "RECORD", "stream": "test", "record": {"str": "value1","int": 1,"decimal": 0.1,"decimal2": null,"date": null,"datetime": "2021-06-11T00:00:00.000000Z","boolean": true}}
{"type": "STATE", "value": {"datetime": "2020-10-19"}}
{"type": "SCHEMA","stream": "test","schema": {"type": "object","properties": {"str": {"type": ["null", "string"]},"int": {"type": ["null", "integer"]},"decimal": {"type": ["null", "number"]},"decimal2": {"type": ["null", "number"]},"date": {"type": ["null", "string"], "format": "date-time"},"datetime": {"type": ["null", "string"], "format": "date-time"},"boolean": {"type": ["null", "boolean"]}}}, "key_properties": ["str"]}
{"type": "RECORD", "stream": "test", "record": {"str": "value2","decimal": 0.2,"decimal2": null,"date": null,"datetime": "2021-06-12T00:00:00.000000Z","boolean": true}}
{"type": "RECORD", "stream": "test", "record": {"str": "value3","int": 3,"decimal": 0.3,"decimal2": null,"date": null,"datetime": "2021-06-13T00:00:00.000000Z","boolean": false}}
{"type": "STATE", "value": {"datetime": "2020-10-19"}}
"""


@pytest.fixture
def input_messages_3_test_null_fields():
    return """\
{"type": "SCHEMA","stream": "test","schema": { "type": ["null", "object"], "properties": { "field1": { "type": ["null", "object"], "additionalProperties": false, "properties": { "field2": { "type": ["null", "object"], "properties": { "field3": { "type": ["null", "string"] }, "field4": { "type": ["null", "string"] } } } } }, "field2": { "type": ["null", "object"], "properties": { "field3": { "type": ["null", "string"] }, "field4": { "type": ["null", "string"] }, "field5": { "type": ["null", "string"] } } }, "field6": { "type": ["null", "string"] } }, "additionalProperties": false }, "key_properties": ["str"]}
{"type": "RECORD", "stream": "test", "record": {"field1": {"field2": {"field3": "test_field3", "field4": "test_field4"}}, "field2": null}}
{"type": "STATE", "value": {"datetime": "2020-10-19"}}
"""


def test_persist_messages(input_messages_1, expected_df_1):
    # content of test_persist.expected.pkl based on : [{"CAD":1.3171828596,"HKD":7.7500212134,"ISK":138.6508273229,"PHP":48.5625795503,"DKK":6.3139584217,"HUF":309.7581671616,"CZK":23.2040729741,"GBP":0.7686720407,"RON":4.1381417056,"SEK":8.7889690284,"IDR":14720.101824353,"INR":73.3088672041,"BRL":5.6121340687,"RUB":77.5902418328,"HRK":6.4340263046,"JPY":105.311837081,"THB":31.1803139584,"CHF":0.9099703012,"EUR":0.8485362749,"MYR":4.1424692406,"BGN":1.6595672465,"TRY":7.8962240136,"CNY":6.6836656767,"NOK":9.2889266016,"NZD":1.5062367416,"ZAR":16.4451421298,"USD":1.0,"MXN":21.0537123462,"SGD":1.3568095036,"AUD":1.4064488757,"ILS":3.3802291048,"KRW":1138.1671616462,"PLN":3.8797624098,"date":"2020-10-19T00:00:00Z"},{"CAD":1.3171828596,"HKD":7.7500212134,"ISK":138.6508273229,"PHP":48.5625795503,"DKK":6.3139584217,"HUF":309.7581671616,"CZK":23.2040729741,"GBP":0.7686720407,"RON":4.1381417056,"SEK":8.7889690284,"IDR":14720.101824353,"INR":73.3088672041,"BRL":5.6121340687,"RUB":77.5902418328,"HRK":6.4340263046,"JPY":105.311837081,"THB":31.1803139584,"CHF":0.9099703012,"EUR":0.8485362749,"MYR":4.1424692406,"BGN":1.6595672465,"TRY":7.8962240136,"CNY":6.6836656767,"NOK":9.2889266016,"NZD":1.5062367416,"ZAR":16.4451421298,"USD":1.0,"MXN":21.0537123462,"SGD":1.3568095036,"AUD":1.4064488757,"ILS":3.3802291048,"KRW":1138.1671616462,"PLN":3.8797624098,"date":"2020-10-19T00:00:00Z"}]

    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")

    input_messages = io.TextIOWrapper(
        io.BytesIO(input_messages_1.encode()), encoding="utf-8"
    )

    with tempfile.TemporaryDirectory() as tmpdirname:
        persist_messages(input_messages, f"{tmpdirname}/test_{timestamp}")
        filename = [f for f in glob.glob(f"{tmpdirname}/test_{timestamp}/*.parquet")]
        df = ParquetFile(filename[0]).read().to_pandas()
        assert_frame_equal(df, expected_df_1, check_like=True)


def test_persist_messages_null_field(input_messages_3_test_null_fields, expected_df_3):
    """
    This tests checks if the null object fields are being correctly exploded according to the schema and
    if it doesn't replace the values if we have a conflict of the same field name in different levels of object.
    """
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    input_messages = io.TextIOWrapper(
        io.BytesIO(input_messages_3_test_null_fields.encode()), encoding="utf-8"
    )
    with tempfile.TemporaryDirectory() as tmpdirname:
        persist_messages(input_messages, f"{tmpdirname}/test_{timestamp}")
        filename = [f for f in glob.glob(f"{tmpdirname}/test_{timestamp}/*.parquet")]
        df = ParquetFile(filename[0]).read().to_pandas()
        assert_frame_equal(df, expected_df_3, check_like=True)


def test_persist_messages_invalid_sort(input_messages_1_reorder):
    input_messages = io.TextIOWrapper(
        io.BytesIO(input_messages_1_reorder.encode()), encoding="utf-8"
    )

    with tempfile.TemporaryDirectory() as tmpdirname:
        with pytest.raises(
            ValueError,
            match="A record for stream test was encountered before a corresponding schema",
        ):
            persist_messages(input_messages, f"{tmpdirname}test_")


def test_persist_with_schema_force(input_messages_2_null_col_with_different_datatype):
    input_messages = io.TextIOWrapper(
        io.BytesIO(input_messages_2_null_col_with_different_datatype.encode()), encoding="utf-8"
    )

    with tempfile.TemporaryDirectory() as tmpdirname:
        persist_messages(input_messages, f"{tmpdirname}/test_force_schema", force_output_schema_cast=True)
        filename = [f for f in glob.glob(f"{tmpdirname}/test_force_schema/*.parquet")]
        schema = pa.parquet.read_schema(filename[0])
        expected_schema = pa.schema([
            pa.field("decimal", pa.float64(), True),
            pa.field("datetime", pa.string(), True),
            pa.field("date", pa.string(), True),
            pa.field("int", pa.int64(), True),
            pa.field("boolean", pa.bool_(), True),
            pa.field("decimal2", pa.float64(), True),
            pa.field("str", pa.string(), True)
        ])
        for field in expected_schema:
            assert schema.field(field.name).type == field.type


def test_create_dataframe():
    with tempfile.TemporaryDirectory() as tmpdir:
        input_tempfile = os.path.join(tmpdir, 'input_tempfile.jsonl')
        pd.DataFrame.from_records([{
            "key_1": 1,
            "key_2__key_3": 2,
            "key_2__key_4__key_5": 3,
            "key_2__key_4__key_6": "['10', '11']",
        }]).to_json(input_tempfile, orient='records', lines=True)

        schema = {
            "key_1": "integer",
            "key_2__key_3": ["null", "string"],
            "key_2__key_4__key_5": ["null", "integer"],
            "key_2__key_4__key_6": "string"
        }

        expected_schema = pa.schema([
            pa.field("key_1", pa.int64(), False),
            pa.field("key_2__key_4__key_6", pa.string(), False),
            pa.field("key_2__key_3", pa.string(), True),
            pa.field("key_2__key_4__key_5", pa.int64(), True)
        ])

        df = create_dataframe(input_tempfile, schema, force_output_schema_cast=True)
        assert sorted(df.column_names) == sorted(expected_schema.names)
        for field in expected_schema:
            assert df.schema.field(field.name).type == field.type
        assert df.num_rows == 1


def test_create_dataframe_no_schema_cast():
    with tempfile.TemporaryDirectory() as tmpdir:
        input_tempfile = os.path.join(tmpdir, 'input_tempfile.jsonl')
        pd.DataFrame.from_records([{
            "key_1": 1,
            "key_2__key_3": 2,
            "key_2__key_4__key_5": 3,
            "key_2__key_4__key_6": "['10', '11']",
        }]).to_json(input_tempfile, orient='records', lines=True)

        schema = {}

        expected_schema = pa.schema([
            pa.field("key_1", pa.int64(), False),
            pa.field("key_2__key_4__key_6", pa.string(), False),
            pa.field("key_2__key_3", pa.int64(), True),
            pa.field("key_2__key_4__key_5", pa.int64(), True)
        ])

        df = create_dataframe(input_tempfile, schema, force_output_schema_cast=False)
        assert sorted(df.column_names) == sorted(expected_schema.names)
        for field in expected_schema:
            assert df.schema.field(field.name).type == field.type
        assert df.num_rows == 1


def test_create_dataframe_exception_no_schema():
    with tempfile.TemporaryDirectory() as tmpdir:
        input_tempfile = os.path.join(tmpdir, 'input_tempfile.jsonl')
        pd.DataFrame.from_records([{
            "key_1": 1,
            "key_2__key_3": 2,
            "key_2__key_4__key_5": 3,
            "key_2__key_4__key_6": "['10', '11']",
        }]).to_json(input_tempfile, orient='records', lines=True)

        schema = {}

        with pytest.raises(Exception, match='Not possible to force the cast because the schema was not provided.'):
            create_dataframe(input_tempfile, schema, force_output_schema_cast=True)
