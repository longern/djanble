import tablestore
import re


def execute(conn: tablestore.OTSClient, sql: str, params):
    delete_regexp = r'\s*DELETE\s+FROM\s+"(?P<table>\S+?)"\s+WHERE\s+".*?"\."id"\s+(?:=\s*%s|IN\s+\(%s(,\s*%s)*\))\s*$'
    delete_match = re.match(delete_regexp, sql)
    if not delete_match:
        raise ValueError(sql)

    table_name = delete_match.groupdict()["table"]
    for param in params:
        primary_keys = [("_partition", 0), ("id", param)]
        consumed, return_row = conn.delete_row(
            table_name, tablestore.Row(primary_keys), tablestore.Condition("EXPECT_EXIST")
        )
