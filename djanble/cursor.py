import importlib
import tablestore


class Cursor:
    """
    Tablestore cursor interface compliant with PEP 249
    https://www.python.org/dev/peps/pep-0249/
    """

    def __init__(self, conn):
        self.conn: tablestore.OTSClient = conn
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
