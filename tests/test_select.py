from djanble.queries.select import parse_select


def test_parse_select():
    parse_result = parse_select(
        'SELECT "oj_problem"."id", "oj_problem"."title" FROM "oj_problem" WHERE "oj_problem"."id" = %s ORDER BY "oj_problem"."title" ASC LIMIT 10'
    )
    assert parse_result["table"] == "oj_problem"
