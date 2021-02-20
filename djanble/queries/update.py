import re
import tablestore


def execute(conn: tablestore.OTSClient, sql: str, params):
    if hasattr(params, "__iter__"):
        params = tuple(bytearray(param) if isinstance(param, memoryview) else param for param in params)

    update_match = re.match('UPDATE "([^ ]*)" SET ((?:"(?:[^"]*)" = (?:%s|NULL),? )+)WHERE ".*"\\."id" = %s$', sql)
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
    conn.update_row(table_name, row, tablestore.Condition("EXPECT_EXIST"))

    return {"rowcount": 1}
