import datetime
import importlib
import re

import boto3

Date = datetime.date

Time = datetime.time

Timestamp = datetime.datetime

Binary = memoryview

# Exceptions
class Error(Exception):
    pass


DataError = type("DataError", (Error,), {})
DatabaseError = type("DatabaseError", (Error,), {})
IntegrityError = type("IntegrityError", (Error,), {})
InterfaceError = type("InterfaceError", (Error,), {})
InternalError = type("InternalError", (Error,), {})
NotSupportedError = type("NotSupportedError", (Error,), {})
OperationalError = type("OperationalError", (Error,), {})
ProgrammingError = type("ProgrammingError", (Error,), {})


class Cursor:
    """
    DynamoDB cursor interface compliant with PEP 249
    https://www.python.org/dev/peps/pep-0249/
    """

    def __init__(self, conn: "Connection"):
        self.conn = conn
        self.rowcount = -1
        self._description_columns = []

    @property
    def description(self):
        return [(column, None, None, None, None, None, None) for column in self._description_columns]

    def execute(self, sql: str, params=None):
        select_match = re.match("\s*SELECT\s+(?P<columns>.*?)\s+FROM", sql, re.IGNORECASE)
        self._description_columns = []
        if isinstance(select_match, re.Match):
            columns_segment: str = select_match.groupdict()["columns"]
            self._description_columns = [
                column.split(".")[-1].strip().strip('"') for column in columns_segment.split(",")
            ]
            sql = re.sub('"[0-9_A-Za-z]+"\.', "", sql)
            # LIMIT clause is not supported
            sql = re.sub('LIMIT\s+\d+', "", sql)

        try:
            statement = sql.split()[0].lower()
            module = importlib.import_module(f"..queries.{statement}", package=__name__)
            retval = module.execute(self.conn, sql, params) or {}
            for key, value in retval.items():
                setattr(self, key, value)
            return
        except ModuleNotFoundError:
            pass

        result = self.conn.client.execute_statement(Statement=sql % params)["Items"]

        if self._description_columns:
            result = [tuple(list(row[column].values())[0] for column in self._description_columns) for row in result]
            self.rowcount = len(result)

        self.result = iter(result)

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

    def close(self):
        pass


class Connection:
    def __init__(self, host: str, user, password, db):
        host_match = re.match(r"^dynamodb\.(?P<region>.*)\.amazonaws\.com$", host, re.IGNORECASE)
        assert host_match, host
        region_name = host_match.groupdict()["region"]
        self.client = boto3.client(
            "dynamodb",
            region_name=region_name,
            aws_access_key_id=user,
            aws_secret_access_key=password,
        )

    def cursor(self) -> Cursor:
        return Cursor(self)


def connect(host, user=None, password=None, db=None):
    return Connection(host, user, password, db)
