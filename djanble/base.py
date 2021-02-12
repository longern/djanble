import logging
import re
from itertools import chain

import sqlparse
import tablestore
from django.db.backends.base.base import BaseDatabaseWrapper
from django.db.backends.base.client import BaseDatabaseClient
from django.db.backends.base.introspection import BaseDatabaseIntrospection
from django.db.backends.base.schema import BaseDatabaseSchemaEditor
from django.db.backends.base.features import BaseDatabaseFeatures
from django.db.backends.base.creation import BaseDatabaseCreation
from django.db.backends.base.operations import BaseDatabaseOperations
from django.db.backends.sqlite3.base import (
    Database,
    DatabaseWrapper as Sqlite3DatabaseWrapper,
)


def do_nothing(*args, **kwargs):
    pass


def row_as_dict(row: tablestore.Row) -> dict:
    row_dict = {}
    for item in chain(row.primary_key, row.attribute_columns):
        key, value, *_ = item
        row_dict[key] = value
    return row_dict


def run_any_select(conn: tablestore.OTSClient, sql: str, params):
    import pandas as pd
    from pandasql import sqldf

    logging.warning("Complex SQL detected. Fetching all data...")
    logging.warning(sql)

    table_names = re.findall('(?:FROM|JOIN) "([^ ]*)"', sql, re.IGNORECASE)

    env = {}
    for table_name in table_names:
        consumed, next_primary_key, row_list, _ = conn.get_range(
            table_name,
            "FORWARD",
            [("_partition", 0), ("id", tablestore.INF_MIN)],
            [("_partition", 0), ("id", tablestore.INF_MAX)],
        )
        table = pd.DataFrame([row_as_dict(row) for row in row_list])
        env[table_name] = table
        # PandaSQL cannot handle table names with quotes
        sql = sql.replace(f'"{table_name}"', table_name)

    result = sqldf(sql % tuple(repr(param) for param in params), env)
    return list(result.itertuples(index=False, name=None))


class Cursor:
    def __init__(self, conn):
        self.conn: tablestore.OTSClient = conn

    def select(self, sql, params):
        sql = re.sub(" LIMIT \d*$", "", sql)
        sql = re.sub(" ORDER BY [^ ]*( (ASC|DESC))?$", "", sql)
        select_match = re.match('SELECT (.*) FROM "([^ ]*)"(?: WHERE ".*"\."(.*)" = %s)?$', sql)
        if not select_match:
            self.result = iter(run_any_select(self.conn, sql, params))
            return

        table_name = select_match.groups()[1]
        columns = [re.sub(".*\.", "", column)[1:-1] for column in select_match.groups()[0].split(", ")]
        condition_column = select_match.groups()[2]
        if condition_column == "id":
            # Get row by id
            _, row, _ = self.conn.get_row(table_name, [("_partition", 0), ("id", params[0])])
            row_list = [row] if row else []
        elif condition_column:
            # Find from index table
            consumed, next_primary_key, row_list, _ = self.conn.get_range(
                f"ix_{table_name}_{condition_column}",
                "FORWARD",
                [(condition_column, params[0]), ("_partition", 0), ("id", tablestore.INF_MIN)],
                [(condition_column, params[0]), ("_partition", 0), ("id", tablestore.INF_MAX)],
            )
        else:
            # Find from main table
            consumed, next_primary_key, row_list, _ = self.conn.get_range(
                table_name,
                "FORWARD",
                [("_partition", 0), ("id", tablestore.INF_MIN)],
                [("_partition", 0), ("id", tablestore.INF_MAX)],
            )

        result = []
        for row in row_list:
            row_dict = row_as_dict(row)
            result.append([row_dict.get(column, None) for column in columns])

        self.rowcount = len(result)
        self.result = iter(result)

    def insert(self, sql: str, params):
        if hasattr(params, "__iter__"):
            params = tuple(bytearray(param) if isinstance(param, memoryview) else param for param in params)

        (stmt,) = sqlparse.parse(sql)

        table_name: str = stmt.tokens[4].get_name()
        column_tokens = stmt.tokens[6].tokens[1].tokens
        columns = [token.get_name() for token in column_tokens if isinstance(token, sqlparse.sql.Identifier)]

        primary_keys = [("_partition", 0), ("id", tablestore.PK_AUTO_INCR)]
        attribute_columns = [(column, param) for column, param in zip(columns, params) if param is not None]
        consumed, return_row = self.conn.put_row(
            table_name,
            tablestore.Row(primary_keys, attribute_columns),
            return_type=tablestore.ReturnType.RT_PK,
        )

        self.lastrowid = row_as_dict(return_row)["id"]
        self.rowcount = 1

    def update(self, sql: str, params):
        if hasattr(params, "__iter__"):
            params = tuple(bytearray(param) if isinstance(param, memoryview) else param for param in params)

        update_match = re.match('UPDATE "([^ ]*)" SET ((?:"(?:[^"]*)" = (?:%s|NULL),? )+)WHERE ".*"\."id" = %s$', sql)
        if not update_match:
            raise ValueError(sql)

        params_iter = iter(params)

        assignments = {}
        for assignment_expr in update_match.groups()[1].split(","):
            lhs, rhs = assignment_expr.strip().split(" = ")
            if rhs == "%s":
                assignments[lhs.strip('"')] = next(params_iter)

        table_name = update_match.groups()[0]
        primary_keys = [("_partition", 0), ("id", params[-1])]
        row = tablestore.Row(primary_keys, {"PUT": list(assignments.items())})
        self.conn.update_row(table_name, row, tablestore.Condition("EXPECT_EXIST"))

        self.rowcount = 1

    def delete(self, sql: str, params):
        delete_match = re.match('DELETE FROM "([^ ]*)" WHERE ".*"\."id" = %s$', sql)
        if not delete_match:
            raise ValueError(sql)

        table_name = delete_match.groups()[0]
        primary_keys = [("_partition", 0), ("id", params[0])]
        consumed, return_row = self.conn.delete_row(
            table_name, tablestore.Row(primary_keys), tablestore.Condition("EXPECT_EXIST")
        )

    def execute(self, sql: str, params=None):
        self.result = []
        statement = sql.split()[0].lower()
        print(sql, params)

        if not hasattr(self, statement):
            raise ValueError("Statement not supported.")

        return getattr(self, statement)(sql, params)

    def fetchmany(self, size=1):
        ret = []
        for _ in range(size):
            try:
                ret.append(next(self.result))
            except StopIteration:
                break
        return ret

    def fetchone(self):
        try:
            return next(self.result)
        except StopIteration:
            return None

    close = do_nothing


class DatabaseIntrospection(BaseDatabaseIntrospection):
    def table_names(self, cursor: Cursor, include_views=False):
        return cursor.conn.list_table()


class DatabaseOperations(BaseDatabaseOperations):
    def quote_name(self, name):
        if name.startswith('"') and name.endswith('"'):
            return name
        return '"{}"'.format(name)


class DatabaseFeatures(BaseDatabaseFeatures):
    uses_savepoints = False
    atomic_transactions = False


class DatabaseWrapper(BaseDatabaseWrapper):
    introspection_class = DatabaseIntrospection
    client_class = BaseDatabaseClient
    creation_class = BaseDatabaseCreation
    features_class = DatabaseFeatures
    ops_class = DatabaseOperations

    Database = Database
    operators = Sqlite3DatabaseWrapper.operators

    SchemaEditorClass = BaseDatabaseSchemaEditor

    def get_connection_params(self) -> None:
        settings_dict: dict = self.settings_dict
        protocol = "https"
        kwargs = {
            "end_point": f"{protocol}://{settings_dict['HOST']}",
            "access_key_id": self.settings_dict["USER"],
            "access_key_secret": self.settings_dict["PASSWORD"],
            "instance_name": self.settings_dict["NAME"],
        }
        return kwargs

    def get_new_connection(self, conn_params):
        self.conn = tablestore.OTSClient(**conn_params)
        return self.conn

    def create_cursor(self, name) -> None:
        return Cursor(self.conn)

    def rollback(self) -> None:
        self.needs_rollback = False

    init_connection_state = do_nothing
    set_autocommit = do_nothing
    commit = do_nothing
    validate_no_broken_transaction = do_nothing
    close = do_nothing
