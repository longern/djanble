import datetime
import importlib

import tablestore

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
    Tablestore cursor interface compliant with PEP 249
    https://www.python.org/dev/peps/pep-0249/
    """

    def __init__(self, conn: "Connection"):
        self.conn = conn
        self.rowcount = -1

    def execute(self, sql: str, params=None):
        statement = sql.split()[0].lower()
        print(sql, params)
        try:
            module = importlib.import_module(f"..queries.{statement}", package=__name__)
        except ModuleNotFoundError:
            raise ValueError(f"Statement {statement} not supported.")

        self.result = []
        result = module.execute(self.conn, sql, params)
        if isinstance(result, dict):
            for key, value in result.items():
                setattr(self, key, value)

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


class Connection(tablestore.OTSClient):
    def __init__(self, host, user, password, db):
        protocol = "https"
        kwargs = {
            "end_point": f"{protocol}://{host}",
            "access_key_id": user,
            "access_key_secret": password,
            "instance_name": db,
        }
        super().__init__(**kwargs)

    def cursor(self) -> Cursor:
        return Cursor(self)


def connect(host, user, password, db):
    return Connection(host, user, password, db)
