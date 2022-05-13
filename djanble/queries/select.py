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

    # Run query with sqlite3
    sqlite_conn = sqlite3.connect(":memory:")
    cursor = sqlite_conn.cursor()

    # Mapping from tablestore type to sqlite type
    column_type_mapping = {
        "STRING": "text",
        "INTEGER": "bigint",
        "BOOLEAN": "boolean",
        "BINARY": "blob",
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
    try:
        sql = re.sub(" LIMIT \\d*$", "", sql)
        sql = re.sub(" ORDER BY [^ ]*( (ASC|DESC))?$", "", sql)
        select_match = re.match('SELECT (.*) FROM "([^ ]*)"(.*)$', sql)
        if not select_match:
            raise ValueError()

        if not select_match.group(3):
            condition_column = None
        else:
            where_match = re.match(' WHERE ".*?"\\."(.*?)" (.*)', select_match.group(3))
            if not where_match:
                raise ValueError()

            condition_column = where_match.group(1)
            if not re.match("(= %s)|(IN \\(%s(, ?%s)*\\))$", where_match.group(2)):
                raise ValueError()

    except ValueError:
        result = run_any_select(conn, sql, params)
        return {"rowcount": len(result), "result": iter(result)}

    table_name = select_match.group(2)
    columns = [re.sub(".*\\.", "", column)[1:-1] for column in select_match.group(1).split(", ")]
    if condition_column == "id" and len(params) == 1:
        # Get row by id
        _, row, _ = conn.get_row(table_name, [("_partition", 0), ("id", params[0])])
        row_list = [row] if row else []
    elif condition_column == "id" and len(params) > 1:
        # Batch get row by id
        request = tablestore.BatchGetRowRequest()
        request.add(
            tablestore.TableInBatchGetRowItem(
                table_name,
                [[("_partition", 0), ("id", param)] for param in params],
                max_version=1
            )
        )

        response = conn.batch_get_row(request)
        table_result = response.get_result_by_table(table_name)
        row_list = [item.row for item in table_result if item.is_ok and item.row]
    elif condition_column:
        # Find from index table
        consumed, next_primary_key, row_list, _ = conn.get_range(
            f"ix_{table_name}_{condition_column}",
            "FORWARD",
            [(condition_column, params[0]), ("_partition", 0), ("id", tablestore.INF_MIN)],
            [(condition_column, params[0]), ("_partition", 0), ("id", tablestore.INF_MAX)],
        )
    else:
        # Get all from main table
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
