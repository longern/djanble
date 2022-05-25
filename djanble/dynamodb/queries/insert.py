import re
import time


def execute(conn, sql: str, params):
    create_regexp = r'\s*INSERT\s+INTO\s+"?(?P<table>\S*?)"?\s+\((?P<columns>[0-9A-Za-z_,"\s]*)\)\s+VALUES\s+\(%s(?:,\s*%s)*\)\s*;?\s*$'
    create_match = re.match(create_regexp, sql, re.IGNORECASE)
    if not create_match:
        raise ValueError(sql)

    table = create_match.groupdict()["table"]
    columns = [s.strip().strip('"') for s in create_match.groupdict()["columns"].split(",")]
    items = dict(zip(columns, params))
    items["_pid"] = 0
    items["id"] = time.time_ns()
    conn.client.execute_statement(Statement=f'INSERT INTO "{table}" VALUE {items}')

    return {"lastrowid": items["id"]}
