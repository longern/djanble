import tablestore
import re


def execute(conn: tablestore.OTSClient, sql: str, params):
    delete_match = re.match('DELETE FROM "([^ ]*)" WHERE ".*"\\."id" = %s$', sql)
    if not delete_match:
        raise ValueError(sql)

    table_name = delete_match.groups()[0]
    primary_keys = [("_partition", 0), ("id", params[0])]
    consumed, return_row = conn.delete_row(
        table_name, tablestore.Row(primary_keys), tablestore.Condition("EXPECT_EXIST")
    )
