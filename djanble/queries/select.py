import logging
import re
from itertools import chain
from typing import List

import tablestore
from django.utils.dateparse import parse_datetime


def row_as_dict(row: tablestore.Row) -> dict:
    row_dict = {}
    for item in chain(row.primary_key, row.attribute_columns):
        key, value, *_ = item
        if isinstance(value, str):
            value_datetime = parse_datetime(value)
            if value_datetime:
                value = value_datetime
        row_dict[key] = value
    return row_dict


def run_any_select(conn: tablestore.OTSClient, sql: str, params) -> list:
    import sqlite3

    logging.warning("Complex SQL detected. Fetching all data...")
    logging.warning(sql)

    sqlite_conn = sqlite3.connect(":memory:")
    cursor = sqlite_conn.cursor()

    column_type_mapping = {
        "STRING": "text",
        "INTEGER": "bigint",
        "BOOLEAN": "boolean",
    }

    table_names = re.findall('(?:FROM|JOIN) "([^ ]*)"', sql, re.IGNORECASE)
    for table_name in table_names:
        # Create table in sqlite
        table_meta = conn.describe_table(table_name).table_meta
        columns = list(chain(table_meta.schema_of_primary_key, table_meta.defined_columns))
        column_tokens = ", ".join(
            f'"{column_name}" {column_type_mapping[column_type]}' for column_name, column_type, *_ in columns
        )
        cursor.execute(f"CREATE TABLE {table_name}({column_tokens})")

        primary_key = [("_partition", 0), ("id", tablestore.INF_MIN)]
        rows: List[tablestore.Row] = []
        while primary_key:
            consumed, primary_key, row_list, _ = conn.get_range(
                table_name,
                "FORWARD",
                primary_key,
                [("_partition", 0), ("id", tablestore.INF_MAX)],
            )
            rows.extend(row_list)

        placeholder_tokens = ", ".join(["?"] * len(columns))
        for row in rows:
            row_dict = row_as_dict(row)
            row_values = [row_dict.get(column_name, None) for column_name, *_ in columns]
            cursor.execute(f'INSERT INTO "{table_name}" VALUES ({placeholder_tokens})', row_values)

    cursor.execute(sql.replace("%s", "?"), params)
    return cursor.fetchall()


def execute(conn: tablestore.OTSClient, sql: str, params):
    sql = re.sub(" LIMIT \\d*$", "", sql)
    sql = re.sub(" ORDER BY [^ ]*( (ASC|DESC))?$", "", sql)
    select_match = re.match('SELECT (.*) FROM "([^ ]*)"(?: WHERE ".*"\\."(.*)" = %s)?$', sql)
    if not select_match:
        result = run_any_select(conn, sql, params)
        return {"rowcount": len(result), "result": iter(result)}

    table_name = select_match.groups()[1]
    columns = [re.sub(".*\\.", "", column)[1:-1] for column in select_match.groups()[0].split(", ")]
    condition_column = select_match.groups()[2]
    if condition_column == "id":
        # Get row by id
        _, row, _ = conn.get_row(table_name, [("_partition", 0), ("id", params[0])])
        row_list = [row] if row else []
    elif condition_column:
        # Find from index table
        consumed, next_primary_key, row_list, _ = conn.get_range(
            f"ix_{table_name}_{condition_column}",
            "FORWARD",
            [(condition_column, params[0]), ("_partition", 0), ("id", tablestore.INF_MIN)],
            [(condition_column, params[0]), ("_partition", 0), ("id", tablestore.INF_MAX)],
        )
    else:
        # Find from main table
        consumed, next_primary_key, row_list, _ = conn.get_range(
            table_name,
            "FORWARD",
            [("_partition", 0), ("id", tablestore.INF_MIN)],
            [("_partition", 0), ("id", tablestore.INF_MAX)],
        )

    result = []
    for row in row_list:
        row_dict = row_as_dict(row)
        result.append([row_dict.get(column, None) for column in columns])

    return {"rowcount": len(result), "result": iter(result)}
