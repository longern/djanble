from .db import connection


def test_create():
    cursor = connection.create_cursor()
    cursor.execute(
        """
        CREATE TABLE "test_djanble_create" (
            "id" integer NOT NULL PRIMARY KEY AUTOINCREMENT,
            "str" varchar(255) NOT NULL,
            "dt" datetime NOT NULL,
            "int" integer NULL,
            "bool" boolean NULL,
            "foreign" integer NULL REFERENCES "test_djanble_2" ("id") DEFERRABLE INITIALLY DEFERRED
        );
        """
    )

    assert "test_djanble_create" in connection.introspection.table_names(cursor)

    cursor.execute(
        """
        DROP TABLE "test_djanble_create";
        """
    )
