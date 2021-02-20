import tablestore
import re


def execute(conn: tablestore.OTSClient, sql: str, params):
    drop_match = re.match(r'\s*DROP TABLE "([^ ]*)"', sql)
    if not drop_match:
        raise ValueError(sql)

    table_name = drop_match.groups()[0]
    conn.delete_table(table_name)
