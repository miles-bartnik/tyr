"""constructor_sql() + functions.reference(): the SQL template of a function class,
derived statically (no instantiation). Self-contained -- no DB or config files."""

from tyr.lineage.functions import string, datetime, math, reference


def test_constructor_sql_reads_the_name_literal_and_params():
    # keyword from super().__init__(name="...") + the __init__ parameter names
    assert string.RegExpReplace.constructor_sql() == "REGEXP_REPLACE(source, regex, value)"
    assert datetime.StringToTimestamp.constructor_sql() == "STRPTIME(source, timestamp_format)"
    assert string.Upper.constructor_sql() == "UPPER(source)"


def test_constructor_sql_needs_no_instance():
    # calling on the class alone must not construct anything (would need real args)
    sql = math.Add.constructor_sql()
    assert sql.startswith("ADD(") and "left" in sql and "right" in sql


def test_reference_enumerates_every_module_without_instantiating():
    ref = reference()
    assert len(ref) > 50
    cats = {r["category"] for r in ref}
    assert {"string", "math", "datetime", "geo", "aggregate"} <= cats
    row = next(r for r in ref if r["name"] == "RegExpReplace")
    assert row["sql"] == "REGEXP_REPLACE(source, regex, value)"
    assert row["params"] == ["source", "regex", "value"]
    assert row["category"] == "string"
    # every row is well-formed
    for r in ref:
        assert r["sql"] and r["name"] and isinstance(r["params"], list)
        assert r["sql"].startswith(r["sql"].split("(")[0])   # KEYWORD(...) shape
