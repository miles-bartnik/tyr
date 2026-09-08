import json
import warnings

from ..units.core import Unit

from .. import core as lineage
from .. import values as lineage_values
from .. import columns as lineage_columns
from .. import functions as lineage_functions
from .. import expressions as lineage_expressions
from . import functions as macro_functions
from . import values as macro_values


def select_primary_key(
    table: lineage._Table, filter_unit: Unit = None, filter_regex: str = None
):
    select_columns = lineage.ColumnList(
        [
            lineage_columns.Core(
                name=column.name,
                source=lineage_columns.Select(column),
            )
            for column in table.primary_key.list_columns(
                filter_regex=filter_regex, filter_unit=filter_unit
            )
        ]
    )

    return select_columns


def select_static_primary_key(
    table: lineage._Table, filter_unit: Unit = None, filter_regex: str = None
):
    select_columns = lineage.ColumnList(
        [
            lineage_columns.Core(
                name=column.name,
                source=lineage_columns.Select(column),
            )
            for column in table.static_primary_key.list_columns(
                filter_regex=filter_regex, filter_unit=filter_unit
            )
        ]
    )

    return select_columns


def select_all(
    table: lineage._Table,
    filter_regex: str = None,
    filter_unit: Unit = None,
    primary_key: bool = True,
    static_primary_key: bool = False,
    apply_filters_to_primary_key: bool = True,
):
    if primary_key and static_primary_key:
        warnings.warn(
            "Both primary_key and static_primary_key selected. Defaulting to primary_key"
        )

    if primary_key and table.primary_key:
        if apply_filters_to_primary_key:
            pk_columns = select_primary_key(
                table, filter_unit=filter_unit, filter_regex=filter_regex
            )
        else:
            pk_columns = select_primary_key(table)
    elif static_primary_key and table.static_primary_key:
        if apply_filters_to_primary_key:
            pk_columns = select_static_primary_key(
                table, filter_unit=filter_unit, filter_regex=filter_regex
            )
        else:
            pk_columns = select_primary_key(table)
    else:
        pk_columns = lineage.ColumnList([])

    select_columns = lineage.ColumnList(
        [
            lineage_columns.Core(
                name=column.name,
                source=lineage_columns.Select(column),
            )
            for column in table.columns.list_columns(
                filter_regex=filter_regex, filter_unit=filter_unit
            )
            if column.name not in table.primary_key.list_names()
        ]
    )

    return pk_columns + select_columns


def staging_column_transform(source_column: lineage_columns.WildCard, column_metadata):
    if "lineage.schema.source.ColumnMetadata" not in str(type(column_metadata)):
        raise ValueError("column_metadata must be ColumnMetadata object")

    source_unit = column_metadata.source_unit
    target_unit = column_metadata.target_unit

    # Initial Read

    source_column = lineage_functions.utility.SourceWildToStagingColumn(
        source_column, column_metadata
    )

    # Casting

    # Use TryCast so that nulls and filter-matched values survive the initial cast
    # and are handled explicitly below (FAIL errors, SKIP becomes a WHERE clause,
    # DEFAULT swaps in the default value).
    CastFunction = lineage_functions.data_type.TryCast

    if column_metadata.data_type == lineage_values.Datatype("INTERVAL"):
        if column_metadata.regex:
            interval_regex = column_metadata.regex

            for row in lineage.DATE_SPECIFIERS:
                if row["interval_translate"]:
                    if (row["interval_translate"]["regex"] != "") and (
                        row["specifier"] in interval_regex
                    ):
                        interval_regex = interval_regex.replace(
                            row["specifier"], row["interval_translate"]["regex"]
                        )

            interval_regex = lineage_values.Varchar(interval_regex)

            source_column = CastFunction(
                lineage.CaseWhen(
                    conditions=[
                        lineage.Condition(
                            checks=[
                                lineage_functions.string.RegExpMatch(
                                    CastFunction(
                                        source_column,
                                        lineage_values.Datatype("VARCHAR"),
                                    ),
                                    interval_regex,
                                )
                            ]
                        )
                    ],
                    values=[
                        lineage_functions.string.RegExpExtract(
                            CastFunction(
                                source_column, lineage_values.Datatype("VARCHAR")
                            ),
                            interval_regex,
                        )
                    ],
                    else_value=lineage_values.Null(data_type=column_metadata.data_type),
                ),
                data_type=lineage_values.Datatype("INTERVAL"),
            )
        else:
            source_column = CastFunction(
                source=source_column, data_type=lineage_values.Datatype("INTERVAL")
            )

    elif column_metadata.regex and column_metadata.var_type not in [
        "timestamp",
        "datetime",
        "date",
    ]:
        source_column = lineage.CaseWhen(
            conditions=[
                lineage.Condition(
                    checks=[
                        lineage_functions.string.RegExpMatch(
                            CastFunction(
                                source_column, lineage_values.Datatype("VARCHAR")
                            ),
                            lineage_values.Varchar(column_metadata.regex),
                        )
                    ]
                )
            ],
            values=[
                lineage_functions.string.RegExpExtract(
                    CastFunction(source_column, lineage_values.Datatype("VARCHAR")),
                    lineage_values.Varchar(column_metadata.regex),
                )
            ],
            else_value=lineage_values.Null(data_type=column_metadata.data_type),
        )

    elif column_metadata.regex != "" and column_metadata.var_type in ("timestamp", "datetime"):
        if (column_metadata.regex == "datetime") & (
            column_metadata.data_type.value == "TIMESTAMP"
        ):
            source_column = CastFunction(
                source=source_column,
                data_type=lineage_values.Datatype("TIMESTAMP"),
            )

        elif (
            column_metadata.data_type.value == "TIMESTAMP"
            and column_metadata.regex == "epoch_ms"
        ):
            source_column = lineage_functions.datetime.EpochMSToTimestamp(
                CastFunction(
                    source=source_column,
                    data_type=lineage_values.Datatype("INTEGER"),
                ),
            )
        elif (
            column_metadata.data_type.value == "TIMESTAMP"
            and column_metadata.regex == "epoch_s"
        ):
            source_column = lineage_functions.datetime.EpochToTimestamp(
                CastFunction(
                    source=source_column,
                    data_type=lineage_values.Datatype("INTEGER"),
                ),
            )
        elif column_metadata.regex[-5:] == "%S.%g":
            source_column = lineage_functions.datetime.StringToTimestamp(
                macro_functions.datetime.zero_pad_timestamp(
                    CastFunction(
                        source=source_column,
                        data_type=lineage_values.Datatype("VARCHAR"),
                    )
                ),
                timestamp_format=lineage_values.Varchar(column_metadata.regex),
            )
        else:
            source_column = lineage_functions.datetime.StringToTimestamp(
                CastFunction(
                    source=source_column,
                    data_type=lineage_values.Datatype("VARCHAR"),
                ),
                timestamp_format=lineage_values.Varchar(column_metadata.regex),
            )

    elif (column_metadata.regex != "") and (column_metadata.var_type == "date"):
        date_regex = column_metadata.regex

        for row in lineage.DATE_SPECIFIERS:
            if row["interval_translate"]:
                if (row["interval_translate"]["regex"] != "") and (
                    row["specifier"] in date_regex
                ):
                    date_regex = date_regex.replace(
                        row["specifier"], row["interval_translate"]["regex"]
                    )

        date_regex = lineage_values.Varchar(date_regex)

        source_column = CastFunction(
            lineage_functions.datetime.StringToTimestamp(
                lineage_functions.string.RegExpExtract(
                    source=CastFunction(
                        source=source_column,
                        data_type=lineage_values.Datatype("VARCHAR"),
                    ),
                    regex=date_regex,
                ),
                lineage_values.Varchar(column_metadata.regex),
            ),
            column_metadata.data_type,
        )

    elif (column_metadata.regex != "") and (
        column_metadata.data_type.value == "VARCHAR"
    ):
        source_column = lineage_functions.string.RegExpExtract(
            source=CastFunction(
                source=source_column,
                data_type=lineage_values.Datatype("VARCHAR"),
            ),
            regex=lineage_values.Varchar(column_metadata.regex),
        )

    elif column_metadata.regex != "":
        source_column = CastFunction(
            source=lineage_functions.string.RegExpExtract(
                source=CastFunction(
                    source=source_column,
                    data_type=lineage_values.Datatype("VARCHAR"),
                ),
                regex=lineage_values.Varchar(column_metadata.regex),
            ),
            data_type=column_metadata.data_type,
        )
    else:
        # For timestamp / datetime columns with no strptime format provided,
        # use a plain CAST so DuckDB raises a conversion error instead of the
        # silent NULL that TRY_CAST would return. This makes format mismatches
        # visible and points the user at the regex (strptime format) field.
        if column_metadata.var_type in ("timestamp", "datetime"):
            source_column = lineage_functions.data_type.Cast(
                source=source_column,
                data_type=column_metadata.data_type,
            )
        else:
            source_column = CastFunction(
                source=source_column,
                data_type=column_metadata.data_type,
            )
    # Filtering

    # Value filtering.
    # PASS / WARN / SKIP do no value-level transform. SKIP becomes a WHERE clause
    # in staging_table_transform using the column attributes set at the end of this
    # function. FAIL raises a runtime error. DEFAULT swaps in the configured default.
    if column_metadata.filter_values:
        if column_metadata.on_filter == "FAIL":
            source_column = lineage.CaseWhen(
                conditions=[
                    lineage.Condition(
                        checks=[
                            lineage_expressions.In(
                                source_column,
                                lineage_values.List(
                                    [
                                        lineage_values.Varchar(value)
                                        for value in column_metadata.filter_values
                                    ]
                                ),
                            )
                        ]
                    )
                ],
                values=[
                    lineage_functions.utility.Error(
                        lineage_values.Varchar(
                            rf"""
                    filter_value in [{', '.join(["'" + filter_value + "'" for filter_value in column_metadata.filter_values])}] encountered. Triggered on_filter=FAIL"""
                        )
                    )
                ],
                else_value=source_column,
            )
        elif (
            column_metadata.on_filter == "DEFAULT"
            and column_metadata.default_value
            != lineage_values.Null(column_metadata.data_type)
        ):
            source_column = lineage.CaseWhen(
                conditions=[
                    lineage.Condition(
                        checks=[
                            lineage_expressions.In(
                                source_column,
                                lineage_values.List(
                                    [
                                        lineage_values.Varchar(value)
                                        for value in column_metadata.filter_values
                                    ]
                                ),
                            )
                        ]
                    )
                ],
                values=[column_metadata.default_value],
                else_value=source_column,
            )
        # PASS, SKIP, WARN: no value-level transform.

    # Null filtering.
    # PASS / WARN / SKIP do no value-level transform. SKIP becomes a WHERE clause
    # in staging_table_transform. FAIL raises a runtime error. DEFAULT swaps in the
    # configured default when the value is NULL.
    if column_metadata.on_null == "FAIL":
        source_column = lineage.CaseWhen(
            conditions=[
                lineage.Condition(
                    checks=[
                        lineage_expressions.Is(
                            source_column,
                            lineage_values.Null(data_type=column_metadata.data_type),
                        )
                    ]
                )
            ],
            values=[
                lineage_functions.utility.Error(
                    lineage_values.Varchar(
                        rf"NULL encountered in column: {column_metadata.column_name}"
                    )
                )
            ],
            else_value=source_column,
        )
    elif (
        column_metadata.on_null == "DEFAULT"
        and column_metadata.default_value
        != lineage_values.Null(column_metadata.data_type)
    ):
        source_column = lineage.CaseWhen(
            conditions=[
                lineage.Condition(
                    checks=[
                        lineage_expressions.Is(
                            source_column,
                            lineage_values.Null(data_type=column_metadata.data_type),
                        )
                    ]
                )
            ],
            values=[column_metadata.default_value],
            else_value=source_column,
        )
    # PASS, SKIP, WARN: no value-level transform.

    if column_metadata.scale_factor:
        if column_metadata.scale_factor != 1:
            source_column = lineage_functions.math.Multiply(
                source_column,
                lineage_values.FloatingPoint(column_metadata.scale_factor),
            )

    if (source_unit != target_unit) and target_unit.name != "":
        source_column = macro_functions.unit_conversion.convert_to_unit(
            source=source_column, target_unit=target_unit
        )

    # if column_metadata.

    if column_metadata.precision:
        if column_metadata.precision[-2:] == "sf":
            source_column = macro_functions.numeric.significant_figures(
                source_column,
                lineage_values.Integer(int(column_metadata.precision[:-2])),
            )
        elif column_metadata.precision[-2:] == "dp":
            source_column = lineage_functions.math.Round(
                source_column,
                lineage_values.Integer(int(column_metadata.precision[:-2])),
            )

    if column_metadata.column_alias:
        column_name = column_metadata.column_alias
    else:
        column_name = column_metadata.column_name

    output = lineage_columns.Core(source=source_column, name=column_name)

    setattr(output, "on_null", column_metadata.on_null)
    setattr(output, "filter_values", column_metadata.filter_values)
    setattr(output, "on_filter", column_metadata.on_filter)
    setattr(output, "is_primary_key", column_metadata.is_primary_key)
    setattr(output, "is_event_time", column_metadata.is_event_time)
    setattr(output, "var_type", column_metadata.var_type)

    return output


def _link_literal(value: str) -> lineage_values.Varchar:
    # values_varchar renders CAST('{value}' AS VARCHAR) without escaping, so double
    # single quotes here to bind the mapping strings as properly escaped literals.
    return lineage_values.Varchar(str(value).replace("'", "''"))


def parse_link_mapping(link_mapping: str):
    """
    Parse a link_mapping string of the form::

        {"A":"value_1", "B":"value_2"} | "null_value"

    The ``| "null_value"`` fallback segment is optional and its quotes optional.
    Returns ``(mapping, null_value)`` where ``mapping`` is a ``dict`` of strings to
    strings and ``null_value`` is ``None`` when the segment is omitted. Raises
    ``ValueError`` when the dict part does not parse or is not str -> str.
    """
    if not link_mapping or not str(link_mapping).strip():
        raise ValueError("link_mapping is empty")

    dict_part = str(link_mapping).strip()
    null_value = None

    if " | " in dict_part:
        dict_part, _, null_part = dict_part.rpartition(" | ")
        null_part = null_part.strip()

        if len(null_part) >= 2 and null_part[0] in "\"'" and null_part[-1] == null_part[0]:
            null_part = null_part[1:-1]

        if not null_part:
            raise ValueError(rf"link_mapping fallback segment is empty: {link_mapping}")

        null_value = null_part

    try:
        mapping = json.loads(dict_part)
    except json.JSONDecodeError as error:
        raise ValueError(rf"link_mapping dict is malformed: {link_mapping}") from error

    if not isinstance(mapping, dict) or not all(
        isinstance(key, str) and isinstance(value, str)
        for key, value in mapping.items()
    ):
        raise ValueError(rf"link_mapping dict must map strings to strings: {link_mapping}")

    if not mapping:
        raise ValueError(rf"link_mapping dict is empty: {link_mapping}")

    return mapping, null_value


def validate_column_links(column_metadata: dict):
    """
    Build-time validation for linked-column config within one dataset's column
    metadata (``{column_name: ColumnMetadata}``). Raises ``ValueError`` on the
    first problem found; called when the staging schema is built.
    """
    for name, metadata in column_metadata.items():
        # getattr: metadata pickled before linking existed has no link fields.
        link_column = getattr(metadata, "link_column", "")

        if not link_column:
            if getattr(metadata, "link_mapping", "").strip():
                raise ValueError(
                    rf"link_mapping is set but link_column is empty: {metadata.dataset}.{name}"
                )
            continue

        if not getattr(metadata, "link_mapping", "").strip():
            raise ValueError(
                rf"link_mapping is required when link_column is set: {metadata.dataset}.{name}"
            )

        parse_link_mapping(metadata.link_mapping)

        if getattr(metadata, "link_behaviour", "") not in ["ON_NULL", "OVERWRITE"]:
            raise ValueError(
                rf"link_behaviour must be ON_NULL or OVERWRITE when link_column is set: {metadata.dataset}.{name}"
            )

        if link_column not in column_metadata:
            raise ValueError(
                rf"link_column '{link_column}' does not exist in dataset '{metadata.dataset}' columns (referenced by {name})"
            )

        link_target = column_metadata[link_column]

        # A SKIP filter removes rows from the staging table, so the lookup key
        # would differ between rows -- reject SKIP-style filtering on a link column.
        if link_target.on_filter == "SKIP" and link_target.filter_values:
            raise ValueError(
                rf"link_column '{link_column}' uses on_filter=SKIP in dataset '{metadata.dataset}' (referenced by {name})"
            )

        if link_target.on_null == "SKIP":
            raise ValueError(
                rf"link_column '{link_column}' uses on_null=SKIP in dataset '{metadata.dataset}' (referenced by {name})"
            )

    # Reject any cycle among linked columns (A -> B -> ... -> A) in this dataset.
    link_graph = {
        name: metadata.link_column
        for name, metadata in column_metadata.items()
        if getattr(metadata, "link_column", "")
    }

    for start in link_graph:
        visited = []
        node = start

        while node in link_graph:
            if node in visited:
                cycle = visited[visited.index(node):] + [node]
                raise ValueError(
                    rf"Cycle detected among linked columns in dataset '{column_metadata[start].dataset}': {' -> '.join(cycle)}"
                )

            visited.append(node)
            node = link_graph[node]


def staging_column_link_transform(column, column_metadata, link_key_expression):
    """
    Linked columns: the third value-transform phase, applied after the null-handling
    phase and after filtering as a final CaseWhen wrapper around the (already
    transformed) column expression, inside the staging SELECT.

    The lookup key is the link column's own transformed staging expression (after its
    null-handling/filtering phases, and including its own link phase when applied in
    dependency order). Key found -> cast(mapped_value AS column data_type); key miss
    -> runtime Error(); NULL key -> the mapping's null_value segment when present,
    otherwise the target is left unchanged. Cast failures raise at runtime.
    """
    mapping, null_value = parse_link_mapping(column_metadata.link_mapping)

    # Compare the key as a string against the mapping keys. Skip the cast when the
    # link column is already VARCHAR.
    if link_key_expression.data_type == lineage_values.Datatype("VARCHAR"):
        link_key = link_key_expression
    else:
        link_key = lineage_functions.data_type.Cast(
            source=link_key_expression,
            data_type=lineage_values.Datatype("VARCHAR"),
        )

    conditions = []
    values = []

    # A NULL link key substitutes the mapping's null_value segment when present,
    # otherwise the target is left unchanged.
    conditions.append(
        lineage.Condition(
            checks=[
                lineage_expressions.Is(
                    link_key,
                    lineage_values.Null(data_type=lineage_values.Datatype("VARCHAR")),
                )
            ]
        )
    )
    if null_value is not None:
        values.append(
            lineage_functions.data_type.Cast(
                _link_literal(null_value), column_metadata.data_type
            )
        )
    else:
        values.append(column)

    for key, mapped_value in mapping.items():
        conditions.append(
            lineage.Condition(
                checks=[
                    lineage_expressions.Equal(link_key, _link_literal(key))
                ]
            )
        )
        values.append(
            lineage_functions.data_type.Cast(
                _link_literal(mapped_value), column_metadata.data_type
            )
        )

    link_case = lineage.CaseWhen(
        conditions=conditions,
        values=values,
        else_value=lineage_functions.utility.Error(
            lineage_values.Varchar(
                rf"Link key from column: {column_metadata.link_column} not found in link_mapping for column: {column_metadata.column_name}"
            )
        ),
    )

    if column_metadata.link_behaviour == "ON_NULL":
        source_column = lineage.CaseWhen(
            conditions=[
                lineage.Condition(
                    checks=[
                        lineage_expressions.Is(
                            column,
                            lineage_values.Null(data_type=column_metadata.data_type),
                        )
                    ]
                )
            ],
            values=[link_case],
            else_value=column,
        )
    else:
        # OVERWRITE: the link always fires.
        source_column = link_case

    output = lineage_columns.Core(source=source_column, name=column.name)

    for attribute in [
        "on_null",
        "filter_values",
        "on_filter",
        "is_primary_key",
        "is_event_time",
        "var_type",
    ]:
        setattr(output, attribute, getattr(column, attribute))

    return output


def apply_column_links(columns, column_metadata_list):
    """
    Apply the linked-column phase to a dataset's already-built staging columns.
    ``columns`` and ``column_metadata_list`` are aligned (same order). Columns are
    wrapped in dependency order so a link key sees the link column's own link phase
    when it has one; the returned list preserves the original column order.
    """
    built_by_name = {
        metadata.column_name: column
        for metadata, column in zip(column_metadata_list, columns)
    }

    wrapped = {}
    result = dict(zip([metadata.column_name for metadata in column_metadata_list], columns))

    linked = [
        metadata
        for metadata in column_metadata_list
        if getattr(metadata, "link_column", "")
    ]

    column_metadata_by_name = {
        metadata.column_name: metadata for metadata in column_metadata_list
    }

    # Dependency order: a linked column is wrapped only after its link target.
    ordered = []
    visited = set()

    def visit(metadata):
        if metadata.column_name in visited:
            return

        visited.add(metadata.column_name)
        target = column_metadata_by_name.get(metadata.link_column)

        if target and getattr(target, "link_column", ""):
            visit(target)

        ordered.append(metadata)

    for metadata in linked:
        visit(metadata)

    for metadata in ordered:
        link_key_expression = wrapped.get(
            metadata.link_column, built_by_name[metadata.link_column]
        )
        result[metadata.column_name] = staging_column_link_transform(
            result[metadata.column_name], metadata, link_key_expression
        )
        wrapped[metadata.column_name] = result[metadata.column_name]

    return [result[metadata.column_name] for metadata in column_metadata_list]
