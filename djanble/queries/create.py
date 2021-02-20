import tablestore
import re
import sqlparse


def execute(conn: tablestore.OTSClient, sql: str, params):
    create_match = re.match(r'\s*CREATE TABLE "([^ ]*)" ([^;]*)', sql)
    if not create_match:
        raise ValueError(sql)

    table_name = create_match.groups()[0]
    schema = create_match.groups()[1]
    primary_keys = [("_partition", "INTEGER"), ("id", "INTEGER", tablestore.PK_AUTO_INCR)]
    table_meta = tablestore.TableMeta(table_name, primary_keys)
    reserved_throughput = tablestore.ReservedThroughput(tablestore.CapacityUnit(0, 0))
    conn.create_table(table_meta, tablestore.TableOptions(), reserved_throughput)
