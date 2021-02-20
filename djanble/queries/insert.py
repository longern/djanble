import sqlparse
import tablestore
from .select import row_as_dict


def execute(conn: tablestore.OTSClient, sql: str, params):
    if hasattr(params, "__iter__"):
        params = tuple(bytearray(param) if isinstance(param, memoryview) else param for param in params)

    (stmt,) = sqlparse.parse(sql)

    table_name: str = stmt.tokens[4].get_name()
    column_tokens = stmt.tokens[6].tokens[1].tokens
    columns = [token.get_name() for token in column_tokens if isinstance(token, sqlparse.sql.Identifier)]

    primary_keys = [("_partition", 0), ("id", tablestore.PK_AUTO_INCR)]
    attribute_columns = [(column, param) for column, param in zip(columns, params) if param is not None]
    consumed, return_row = conn.put_row(
        table_name,
        tablestore.Row(primary_keys, attribute_columns),
        return_type=tablestore.ReturnType.RT_PK,
    )

    return {"lastrowid": row_as_dict(return_row)["id"], "rowcount": 1}
