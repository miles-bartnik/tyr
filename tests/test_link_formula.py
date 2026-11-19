"""Linked-column formula form: link_mapping as a small expression DSL.

Numeric formulas ``[link_column * 2 + 1] | "0"`` evaluate the link column's
transformed staging expression in DOUBLE; string formulas
``{upper(link_column) || '_x'}`` cast the key to VARCHAR first. The
``| "fallback"`` segment substitutes on a NULL link key, matching the dict
form's semantics. See tyr.lineage.link_formula for the full grammar.
"""

import pandas as pd
import pytest
import tyr

from tyr.lineage import core as lineage
from tyr.lineage import link_formula
from tyr.lineage.macros.columns import validate_column_links
from tyr.lineage.schema import source as source_schema
from tyr.lineage.schema import staging as staging_schema

from test_linked_columns import (
    CSV,
    KEY_ROW,
    column_metadata,
    init_connection,
    metadata_row,
    value_row,
    write_config,
    build_staging,
)


# is_link_formula / split_link_formula


def test_is_link_formula_detection():
    assert link_formula.is_link_formula("[link_column]")
    assert link_formula.is_link_formula("  [link_column * 2] | '0'")
    assert link_formula.is_link_formula("{upper(link_column)}")
    # dict form: JSON object keys are always quoted
    assert not link_formula.is_link_formula('{"A":"1"}')
    assert not link_formula.is_link_formula('{"A":"1"} | "9"')


def test_split_link_formula_fallback():
    assert link_formula.split_link_formula('[link_column] | "0"') == (
        "[link_column]",
        "0",
    )
    assert link_formula.split_link_formula("[link_column] | 0") == (
        "[link_column]",
        "0",
    )
    assert link_formula.split_link_formula("[link_column]") == ("[link_column]", None)


def test_split_link_formula_ignores_pipe_inside_string_constant():
    expr, fallback = link_formula.split_link_formula(
        "{replace(link_column, 'a | b', 'x')} | 'fb'"
    )

    assert expr == "{replace(link_column, 'a | b', 'x')}"
    assert fallback == "fb"


def test_split_link_formula_empty_fallback_rejected():
    with pytest.raises(ValueError, match="fallback segment is empty"):
        link_formula.split_link_formula("[link_column] | ")


# Parser: shapes and markers


def test_parse_numeric_mode_and_fallback():
    _, mode, fallback = link_formula.parse_link_formula('[link_column * 2] | "0"')

    assert mode == "numeric"
    assert fallback == "0"


def test_parse_string_mode_no_fallback():
    expression, mode, fallback = link_formula.parse_link_formula(
        "{upper(link_column) || '_x'}"
    )

    assert mode == "string"
    assert fallback is None
    assert "UPPER" in expression.sql
    assert "||" in expression.sql


@pytest.mark.parametrize(
    "mapping",
    [
        "",  # empty
        "[",  # no body
        "[]",
        "[link_column ** 2}",  # mismatched marker
        "[link_column",  # unclosed marker
        "{link_column}}",  # trailing junk after the marker
        "[(link_column + 1]",  # unbalanced parens
        "[link_column + 1))]",  # unbalanced parens
        "[link_column $ 1]",  # bad token
        '[link_column / "a"]',  # double-quoted constants are not legal
        "[link_column] | ",  # empty fallback segment
    ],
)
def test_parse_shape_errors(mapping):
    with pytest.raises(ValueError):
        link_formula.parse_link_formula(mapping)


# Parser: precedence and associativity


def test_precedence_exponent_binds_tighter_than_multiplicative():
    expression, _, _ = link_formula.parse_link_formula("[2 * 3 ** 2]")

    # MULTIPLY is the outer node; POW is nested inside its arguments
    assert "MULTIPLY(2, POW(3, 2))" in expression.sql.replace("\n", "")


def test_precedence_multiplicative_binds_tighter_than_additive():
    expression, _, _ = link_formula.parse_link_formula("[link_column + 2 * 3]")

    assert "ADD" in expression.sql
    assert "MULTIPLY(2, 3)" in expression.sql


def test_parentheses_override_precedence():
    expression, _, _ = link_formula.parse_link_formula("[(link_column + 2) * 3]")

    assert "MULTIPLY(ADD" in expression.sql.replace("\n", "")


def test_exponent_is_right_associative():
    expression, _, _ = link_formula.parse_link_formula("[2 ** 3 ** 2]")

    # 2 ** (3 ** 2), not (2 ** 3) ** 2
    assert "POW(2, POW(3, 2))" in expression.sql.replace("\n", "")


def test_unary_minus():
    expression, _, _ = link_formula.parse_link_formula("[-link_column + 1]")

    assert "SUBTRACT" in expression.sql


def test_modulo_and_division_render():
    expression, _, _ = link_formula.parse_link_formula("[link_column % 2]")

    assert "FMOD" in expression.sql

    expression, _, _ = link_formula.parse_link_formula("[link_column / 2.0]")

    assert "DIVIDE" in expression.sql


# Parser: the link_column identifier resolves against the key expression


def test_link_column_resolves_to_key_expression():
    from tyr.lineage import values as lineage_values

    key = lineage_values.Integer(5)

    expression, _, _ = link_formula.parse_link_formula("[link_column * 2]", key)

    assert "5" in expression.sql
    assert "DOUBLE" in expression.sql


def test_link_column_in_string_mode_casts_key_to_varchar():
    from tyr.lineage import values as lineage_values

    key = lineage_values.Integer(5)

    expression, _, _ = link_formula.parse_link_formula(
        "{link_column || 'x'}", key
    )

    assert "VARCHAR" in expression.sql


def test_unknown_identifier_rejected():
    with pytest.raises(ValueError, match="unknown identifier 'other'"):
        link_formula.parse_link_formula("[other + 1]")


def test_link_column_is_the_only_legal_identifier_even_in_calls():
    with pytest.raises(ValueError, match="unknown identifier 'pi'"):
        link_formula.parse_link_formula("[abs(pi)]")


# Parser: functions


def test_numeric_functions_build():
    cases = {
        "[floor(link_column)]": "FLOOR",
        "[ceil(link_column)]": "CEIL",
        "[round(link_column)]": "ROUND",
        "[round(link_column, 2)]": "ROUND",
        "[abs(link_column)]": "ABS",
        "[min(link_column, 1, 2)]": "LEAST",
        "[max(link_column, 1)]": "GREATEST",
    }

    for mapping, sql_name in cases.items():
        expression, _, _ = link_formula.parse_link_formula(mapping)

        assert sql_name in expression.sql, mapping


def test_string_functions_build():
    cases = {
        "{upper(link_column)}": "UPPER",
        "{lower(link_column)}": "LOWER",
        "{trim(link_column)}": "TRIM",
        "{left(link_column, 2)}": "LEFT",
        "{right(link_column, 2)}": "RIGHT",
        "{length(link_column)}": "LENGTH",
        "{replace(link_column, 'a', 'b')}": "REPLACE",
    }

    for mapping, sql_name in cases.items():
        expression, _, _ = link_formula.parse_link_formula(mapping)

        assert sql_name in expression.sql, mapping


@pytest.mark.parametrize(
    "mapping, match",
    [
        ("[link_column(1)]", "not a function"),  # link_column is not callable
        ("[nope(1)]", "unknown function"),
        ("{nope(1)}", "unknown function"),
        ("[min(link_column)]", "at least 2"),  # arity
        ("[floor(1, 2)]", "takes 1"),
        ("[round(1, 2, 3)]", "argument"),
        ("{left(link_column)}", "takes 2"),
        ("{length(1, 2)}", "takes 1"),
        ("{replace(link_column, 'a')}", "takes 3"),
        ("[upper(link_column)]", "not allowed in a numeric formula"),  # mode
        ("{floor(link_column)}", "not allowed in a string formula"),
    ],
)
def test_function_errors(mapping, match):
    with pytest.raises(ValueError, match=match):
        link_formula.parse_link_formula(mapping)


# Parser: mode consistency


@pytest.mark.parametrize(
    "mapping, match",
    [
        ("[link_column || 'a']", "only allowed in a string formula"),  # || is string-only
        ("{'a' || link_column ** 2}", "only allowed in a numeric formula"),  # ** in string mode
        ("{link_column + 1}", "arithmetic operator"),  # arithmetic in string mode
        ("{link_column * 2}", "arithmetic operator"),
        ("{link_column / 2}", "arithmetic operator"),
        ("{link_column % 2}", "arithmetic operator"),
        ("{'ab'}", None),  # string constants are fine in string mode
        ("['ab' || link_column]", "string constants"),  # strings in numeric mode
        ("[link_column + 'ab']", "string constants"),
    ],
)
def test_mode_consistency(mapping, match):
    if match is None:
        link_formula.parse_link_formula(mapping)
    else:
        with pytest.raises(ValueError, match=match):
            link_formula.parse_link_formula(mapping)


def test_division_by_zero_constant_rejected_at_build_time():
    with pytest.raises(ValueError, match="zero constant"):
        link_formula.parse_link_formula("[link_column / 0]")

    with pytest.raises(ValueError, match="zero constant"):
        link_formula.parse_link_formula("[link_column % 0.0]")


# Build-time validation


def test_validation_passes_for_valid_formula():
    validate_column_links(
        {
            "key": column_metadata(**KEY_ROW),
            "value": column_metadata(
                **value_row(
                    link_column="key",
                    link_mapping="[link_column + 1]",
                    link_behaviour="ON_NULL",
                )
            ),
        }
    )


@pytest.mark.parametrize(
    "link_mapping",
    [
        "[link_column ** 2}",
        "[foo(link_column)]",
        "[floor(link_column, 2)]",
        "[link_column || 'a']",
    ],
)
def test_validation_rejects_bad_formula(link_mapping):
    with pytest.raises(ValueError):
        validate_column_links(
            {
                "key": column_metadata(**KEY_ROW),
                "value": column_metadata(
                    **value_row(
                        link_column="key",
                        link_mapping=link_mapping,
                        link_behaviour="ON_NULL",
                    )
                ),
            }
        )


# Generated SQL structure


def test_formula_case_wraps_after_null_handling(tmp_path):
    source = write_config(
        tmp_path,
        CSV,
        [
            KEY_ROW,
            value_row(
                on_null="DEFAULT",
                default_value="-999",
                link_column="key",
                link_mapping="[link_column + 1] | \"222\"",
                link_behaviour="ON_NULL",
            ),
        ],
    )
    staging = build_staging(source)

    column = staging.tables["links"].columns["value"]

    # The link phase is a final CaseWhen wrapper around the (null-handled) column.
    assert isinstance(column.source, lineage.CaseWhen)

    link_case = column.source.values[0]
    assert isinstance(link_case, lineage.CaseWhen)
    # NULL key hits the fallback segment first.
    assert column.sql.index("'222'") < column.sql.index("ADD")

    # The wrapped (else) branch is the null-handling expression, i.e. the link
    # transform is applied after the null-handling phase, in the same SELECT.
    assert "-999" in column.source.else_value.sql
    assert isinstance(column.source.else_value.source, lineage.CaseWhen)


def test_formula_division_guard_embeds_error(tmp_path):
    source = write_config(
        tmp_path,
        CSV,
        [
            KEY_ROW,
            value_row(
                link_column="key",
                link_mapping="[10 / abs(link_column)]",
                link_behaviour="ON_NULL",
            ),
        ],
    )
    staging = build_staging(source)

    assert "ERROR" in staging.tables["links"].sql
    assert "division by zero" in staging.tables["links"].sql


# Execution semantics (DuckDB)


NUMERIC_KEY_ROW = metadata_row(
    column_name="key", var_type="numeric", data_type="INTEGER", ordinal_position=0
)

NUMERIC_CSV = "key,value\n1,\n2,7\n,5\n"


def numeric_value_row(**overrides):
    return value_row(
        column_name="value",
        var_type="numeric",
        data_type="INTEGER",
        ordinal_position=1,
        **overrides,
    )


def test_formula_numeric_on_null_fires_only_on_null(tmp_path):
    source = write_config(
        tmp_path,
        NUMERIC_CSV,
        [
            NUMERIC_KEY_ROW,
            numeric_value_row(
                link_column="key",
                link_mapping="[link_column * 10]",
                link_behaviour="ON_NULL",
            ),
        ],
    )
    staging = build_staging(source)
    conn = init_connection(source, staging)

    result = conn.execute(
        "SELECT key, value FROM staging.links ORDER BY key NULLS LAST, value"
    ).df()

    # key 1 -> 10 (value was NULL), key 2 -> 7 untouched, NULL key -> 5 untouched
    assert result["value"].tolist() == [10, 7, 5]

    conn.close()


def test_formula_numeric_overwrite_always_fires(tmp_path):
    source = write_config(
        tmp_path,
        NUMERIC_CSV,
        [
            NUMERIC_KEY_ROW,
            numeric_value_row(
                link_column="key",
                link_mapping="[link_column * 10]",
                link_behaviour="OVERWRITE",
            ),
        ],
    )
    staging = build_staging(source)
    conn = init_connection(source, staging)

    result = conn.execute(
        "SELECT key, value FROM staging.links ORDER BY key NULLS LAST, value"
    ).df()

    assert result["value"].tolist() == [10, 20, 5]

    conn.close()


def test_formula_numeric_fallback_on_null_key(tmp_path):
    source = write_config(
        tmp_path,
        "key,value\n,\n3,\n",
        [
            NUMERIC_KEY_ROW,
            numeric_value_row(
                link_column="key",
                link_mapping='[link_column * 2] | "9"',
                link_behaviour="ON_NULL",
            ),
        ],
    )
    staging = build_staging(source)
    conn = init_connection(source, staging)

    result = conn.execute("SELECT key, value FROM staging.links ORDER BY value").df()

    # NULL key -> fallback 9; key 3 -> 3 * 2 = 6
    assert result["value"].tolist() == [6, 9]

    conn.close()


def test_formula_numeric_functions_and_precedence_execute(tmp_path):
    source = write_config(
        tmp_path,
        "key,value\n4,\n6,\n",
        [
            NUMERIC_KEY_ROW,
            numeric_value_row(
                link_column="key",
                link_mapping="[link_column / 2 + 2 ** 3]",  # 4/2+8=10, 6/2+8=11
                link_behaviour="OVERWRITE",
            ),
        ],
    )
    staging = build_staging(source)
    conn = init_connection(source, staging)

    result = conn.execute("SELECT key, value FROM staging.links ORDER BY key").df()

    assert result["value"].tolist() == [10, 11]

    conn.close()


def test_formula_divide_by_zero_raises_runtime_error(tmp_path):
    source = write_config(
        tmp_path,
        "key,value\n0,\n",
        [
            NUMERIC_KEY_ROW,
            numeric_value_row(
                link_column="key",
                link_mapping="[10 / link_column]",
                link_behaviour="ON_NULL",
            ),
        ],
    )
    staging = build_staging(source)
    conn = tyr.database.connections.Connection(
        name="test", syntax="duckdb", database=":memory:"
    )

    conn.execute(source.settings.sql)
    conn.execute(staging.settings.sql)
    tyr.database.core.create_tables(source, conn)

    with pytest.raises(Exception, match="division by zero"):
        tyr.database.core.create_tables(staging, conn)

    conn.close()


def test_formula_bad_cast_raises_runtime_error(tmp_path):
    source = write_config(
        tmp_path,
        CSV,  # VARCHAR keys 'A'/'B'
        [
            KEY_ROW,
            value_row(
                link_column="key",
                link_mapping="[link_column * 2]",
                link_behaviour="ON_NULL",
            ),
        ],
    )
    staging = build_staging(source)
    conn = tyr.database.connections.Connection(
        name="test", syntax="duckdb", database=":memory:"
    )

    conn.execute(source.settings.sql)
    conn.execute(staging.settings.sql)
    tyr.database.core.create_tables(source, conn)

    with pytest.raises(Exception, match="Could not convert"):
        tyr.database.core.create_tables(staging, conn)

    conn.close()


def test_formula_string_mode_executes(tmp_path):
    source = write_config(
        tmp_path,
        CSV,
        [
            KEY_ROW,
            value_row(
                column_name="value",
                var_type="categorical",
                data_type="VARCHAR",
                ordinal_position=1,
                link_column="key",
                link_mapping="{lower(link_column) || '-x'}",
                link_behaviour="OVERWRITE",
            ),
        ],
    )
    staging = build_staging(source)
    conn = init_connection(source, staging)

    result = conn.execute(
        "SELECT key, value FROM staging.links ORDER BY key NULLS LAST, value"
    ).df()

    # A -> 'a-x', B -> 'b-x', NULL key -> value untouched ('5' staged as VARCHAR)
    assert result["value"].tolist() == ["a-x", "b-x", "5"]

    conn.close()


def test_formula_string_mode_with_trim_replace_left_right(tmp_path):
    source = write_config(
        tmp_path,
        "key,value\n  AbC ,\nXyZ,\n",
        [
            KEY_ROW,
            value_row(
                column_name="value",
                var_type="categorical",
                data_type="VARCHAR",
                ordinal_position=1,
                link_column="key",
                link_mapping="{left(trim(link_column), 2)}",
                link_behaviour="OVERWRITE",
            ),
        ],
    )
    staging = build_staging(source)
    conn = init_connection(source, staging)

    result = conn.execute("SELECT key, value FROM staging.links ORDER BY key").df()

    assert result["value"].tolist() == ["Ab", "Xy"]

    conn.close()


def test_formula_length_produces_integer_cast_to_varchar(tmp_path):
    source = write_config(
        tmp_path,
        CSV,
        [
            KEY_ROW,
            value_row(
                column_name="value",
                var_type="categorical",
                data_type="VARCHAR",
                ordinal_position=1,
                link_column="key",
                link_mapping="{length(link_column)}",
                link_behaviour="OVERWRITE",
            ),
        ],
    )
    staging = build_staging(source)
    conn = init_connection(source, staging)

    result = conn.execute(
        "SELECT key, value FROM staging.links ORDER BY key NULLS LAST, value"
    ).df()

    assert result["value"].tolist() == ["1", "1", "5"]

    conn.close()


# Dict form regression: formula support must not change dict behaviour


def test_dict_form_still_detected_and_executes(tmp_path):
    source = write_config(tmp_path, CSV, [KEY_ROW, metadata_row(
        column_name="value",
        var_type="numeric",
        data_type="INTEGER",
        ordinal_position=1,
        link_column="key",
        link_mapping='{"A":"1","B":"2"}',
        link_behaviour="ON_NULL",
    )])
    staging = build_staging(source)
    conn = init_connection(source, staging)

    result = conn.execute(
        "SELECT key, value FROM staging.links ORDER BY key NULLS LAST, value"
    ).df()

    assert result["value"].tolist() == [1, 7, 5]

    conn.close()
