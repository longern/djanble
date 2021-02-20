import environ

from djanble.base import DatabaseWrapper


def get_default_connection() -> DatabaseWrapper:
    env = environ.Env()
    environ.Env.read_env()
    connection = DatabaseWrapper(env.db("DATABASE_URL"))
    connection_params = connection.get_connection_params()
    connection.get_new_connection(connection_params)
    return connection


connection = get_default_connection()
