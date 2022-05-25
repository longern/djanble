import re


def execute(conn, sql: str, params):
    if re.match(r'\s*CREATE\s+INDEX', sql, re.IGNORECASE):
        return  # Not supported

    create_regexp = r'\s*CREATE\s+TABLE\s+"(?P<table>\S*)"\s+\((?P<columns>.*)\)\s*;?\s*$'
    create_match = re.match(create_regexp, sql, re.IGNORECASE)
    if not create_match:
        raise ValueError(sql)

    conn.client.create_table(
        AttributeDefinitions=[
            {"AttributeName": "_pid", "AttributeType": "N"},
            {"AttributeName": "id", "AttributeType": "N"},
        ],
        TableName=create_match.groupdict()["table"],
        KeySchema=[
            {"AttributeName": "_pid", "KeyType": "HASH"},
            {"AttributeName": "id", "KeyType": "RANGE"},
        ],
        ProvisionedThroughput={"ReadCapacityUnits": 1, "WriteCapacityUnits": 1},
    )
