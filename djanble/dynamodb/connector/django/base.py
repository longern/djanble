from django.db.backends.base.base import BaseDatabaseWrapper
from django.db.backends.base.client import BaseDatabaseClient
from django.db.backends.base.creation import BaseDatabaseCreation
from django.db.backends.base.features import BaseDatabaseFeatures
from django.db.backends.base.introspection import BaseDatabaseIntrospection
from django.db.backends.base.operations import BaseDatabaseOperations
from django.db.backends.base.schema import BaseDatabaseSchemaEditor
from django.db.backends.sqlite3.base import DatabaseWrapper as Sqlite3DatabaseWrapper

from djanble.dynamodb import dbapi2 as Database


def do_nothing(*args, **kwargs):
    pass


class DatabaseIntrospection(BaseDatabaseIntrospection):
    def table_names(self, cursor: Database.Cursor, include_views=False):
        return cursor.conn.client.list_tables()["TableNames"]


class DatabaseOperations(BaseDatabaseOperations):
    def quote_name(self, name):
        if name.startswith('"') and name.endswith('"'):
            return name
        return '"{}"'.format(name)


class DatabaseFeatures(BaseDatabaseFeatures):
    uses_savepoints = False
    atomic_transactions = False
    has_bulk_insert = False


class DatabaseWrapper(BaseDatabaseWrapper):
    introspection_class = DatabaseIntrospection
    client_class = BaseDatabaseClient
    creation_class = BaseDatabaseCreation
    features_class = DatabaseFeatures
    ops_class = DatabaseOperations

    Database = Database
    operators = Sqlite3DatabaseWrapper.operators

    SchemaEditorClass = BaseDatabaseSchemaEditor

    def get_connection_params(self) -> None:
        kwargs = {
            "host": self.settings_dict["HOST"],
            "user": self.settings_dict["USER"],
            "password": self.settings_dict["PASSWORD"],
            "db": self.settings_dict["NAME"],
        }
        return kwargs

    def get_new_connection(self, conn_params):
        self.conn = Database.connect(**conn_params)
        return self.conn

    def create_cursor(self, name=None) -> Database.Cursor:
        return self.conn.cursor()

    def rollback(self) -> None:
        self.needs_rollback = False

    init_connection_state = do_nothing
    set_autocommit = do_nothing
    commit = do_nothing
    validate_no_broken_transaction = do_nothing
    close = do_nothing
