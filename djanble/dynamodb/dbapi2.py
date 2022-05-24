import re

import boto3


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

        result = self.conn.client.execute_statement(Statement=sql)["Items"]

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