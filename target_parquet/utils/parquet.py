from __future__ import annotations

import simplejson as json
import logging
from typing import MutableMapping, Any

import pyarrow as pa
import pyarrow.parquet as pq
from singer_sdk.helpers._flattening import flatten_key, _should_jsondump_value

from target_parquet.utils import bytes_to_mb

FIELD_TYPE_TO_PYARROW = {
    "BOOLEAN": pa.bool_(),
    "STRING": pa.string(),
    "ARRAY": pa.string(),
    "INTEGER": pa.int64(),
    "NUMBER": pa.float64(),
    "OBJECT": pa.string(),
}


EXTENSION_MAPPING = {
    "snappy": ".snappy",
    "gzip": ".gz",
    "brotli": ".br",
    "zstd": ".zstd",
    "lz4": ".lz4",
}

logger = logging.getLogger(__name__)


def _field_type_to_pyarrow_field(
    field_name: str, input_types: dict, required_fields: list[str]
) -> pa.Field:
    types = input_types.get("type", [])
    # If type is not defined, check if anyOf is defined
    if not types:
        for any_type in input_types.get("anyOf", []):
            if t := any_type.get("type"):
                if isinstance(t, list):
                    types.extend(t)
                else:
                    types.append(t)
    types = [types] if isinstance(types, str) else types
    types_uppercase = [item.upper() for item in types]
    nullable = "NULL" in types_uppercase or field_name not in required_fields
    if "NULL" in types_uppercase:
        types_uppercase.remove("NULL")
    input_type = next(iter(types_uppercase)) if types_uppercase else ""
    pyarrow_type = FIELD_TYPE_TO_PYARROW.get(input_type, pa.string())
    return pa.field(field_name, pyarrow_type, nullable)


def flatten_schema_to_pyarrow_schema(flatten_schema_dictionary: dict) -> pa.Schema:
    """Function that converts a flatten schema to a pyarrow schema in a defined order.

    E.g:
     dictionary = {
        'properties': {
             'key_1': {'type': ['null', 'integer']},
             'key_2__key_3': {'type': ['null', 'string']},
             'key_2__key_4__key_5': {'type': ['null', 'integer']},
             'key_2__key_4__key_6': {'type': ['null', 'array']}
           }
        }
    By calling the function with the dictionary above as parameter,
    you will get the following structure:
        pa.schema([
             pa.field('key_1', pa.int64()),
             pa.field('key_2__key_3', pa.string()),
             pa.field('key_2__key_4__key_5', pa.int64()),
             pa.field('key_2__key_4__key_6', pa.string())
        ])
    """
    flatten_schema = flatten_schema_dictionary.get("properties", {})
    required_fields = flatten_schema_dictionary.get("required", [])
    return pa.schema(
        [
            _field_type_to_pyarrow_field(
                field_name, field_input_types, required_fields=required_fields
            )
            for field_name, field_input_types in flatten_schema.items()
        ]
    )


def create_pyarrow_table(list_dict: list[dict], schema: pa.Schema) -> pa.Table:
    """Create a pyarrow Table from a python list of dict."""
    data = {f: [row.get(f) for row in list_dict] for f in schema.names}
    return pa.table(data).cast(schema)


def concat_tables(
    records: list[dict], pyarrow_table: pa.Table, pyarrow_schema: pa.Schema
) -> pa.Table:
    """Create a dataframe from records and concatenate with the existing one."""
    if not records:
        return pyarrow_table
    new_table = create_pyarrow_table(records, pyarrow_schema)
    return pa.concat_tables([pyarrow_table, new_table]) if pyarrow_table else new_table


def write_parquet_file(
    table: pa.Table,
    path: str,
    compression_method: str = "gzip",
    basename_template: str | None = None,
    partition_cols: list[str] | None = None,
) -> None:
    """Write a pyarrow table to a parquet file."""
    pq.write_to_dataset(
        table,
        root_path=path,
        compression=compression_method,
        partition_cols=partition_cols or None,
        use_threads=True,
        use_legacy_dataset=False,
        basename_template=f"{basename_template}{EXTENSION_MAPPING[compression_method.lower()]}.parquet"
        if basename_template
        else None,
    )


def get_pyarrow_table_size(table: pa.Table) -> float:
    """Return the size of a pyarrow table in MB."""
    return bytes_to_mb(table.nbytes)


# TODO: Move to singer-sdk after this PR https://github.com/meltano/sdk/pull/2243 is merged
def flatten_record(
    record: dict,
    flattened_schema: dict | None = None,
    max_level: int | None = None,
    separator: str = "__",
) -> dict:
    """Flatten a record up to max_level.

    Args:
        record: The record to flatten.
        flattened_schema: The already flattened schema.
        separator: The string used to separate concatenated key names. Defaults to "__".
        max_level: The maximum depth of keys to flatten recursively.

    Returns:
        A flattened version of the record.
    """
    assert (flattened_schema is not None) or (max_level is not None), "flattened_schema or max_level must be provided"
    max_level = max_level or 0

    return _flatten_record(
        record_node=record,
        flattened_schema=flattened_schema,
        separator=separator,
        max_level=max_level,
    )


def _flatten_record(
    record_node: MutableMapping[Any, Any],
    *,
    flattened_schema: dict | None = None,
    parent_key: list[str] | None = None,
    separator: str = "__",
    level: int = 0,
    max_level: int = 0,
) -> dict:
    """This recursive function flattens the record node.

    The current invocation is expected to be at `level` and will continue recursively
    until the provided `max_level` is reached.

    Args:
        record_node: The record node to flatten.
        flattened_schema: The already flattened full schema for the record.
        parent_key: The parent's key, provided as a list of node names.
        separator: The string to use when concatenating key names.
        level: The current recursion level (zero-based).
        max_level: The max recursion level (zero-based, exclusive).

    Returns:
        A flattened version of the provided node.
    """
    if parent_key is None:
        parent_key = []

    items: list[tuple[str, Any]] = []
    for k, v in record_node.items():
        new_key = flatten_key(k, parent_key, separator)
        if (isinstance(v, MutableMapping) and
                ((flattened_schema and new_key not in flattened_schema.get('properties', {})) or
                 (not flattened_schema and level < max_level))):
            items.extend(
                _flatten_record(
                    v,
                    flattened_schema=flattened_schema,
                    parent_key=[*parent_key, k],
                    separator=separator,
                    level=level + 1,
                    max_level=max_level,
                ).items(),
            )
        else:
            items.append(
                (
                    new_key,
                    json.dumps(v, use_decimal=True)
                    if _should_jsondump_value(k, v, flattened_schema)
                    else v,
                ),
            )

    return dict(items)
