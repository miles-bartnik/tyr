"""Bulk-source ingestion for tyr's programmatic path.

A dataset declared ``bulk`` on ``SourceSettings`` has its rows supplied by
the caller (pyarrow Table, pandas DataFrame, or list of dicts) instead of
being read from a source file. The source schema's staging SQL references
the placeholder table ``_tyr_bulk_<dataset>`` (see
``lineage.schema.source.ReadBulk``); the executor materializes the caller's
rows into that table BEFORE running the schema build and drops it after.

Why a real table and not duckdb's registered views: registered views (and
TEMP tables) are scoped to the DuckDBPyConnection they were created on and
are NOT visible from ``.cursor()`` contexts -- tyr's Connection wrapper
executes every statement through a fresh cursor, so only catalog tables are
visible there. The placeholder is therefore a transient catalog table:
created by register_bulk_sources, dropped by unregister_bulk_sources, and
(on executors that build inside a transaction, e.g. apian's _materialize)
removed by rollback on failure too.

Transport and NULLs: pyarrow tables are the preferred transport -- typed
columns carry real NULLs, which the column_metadata on_null policies then
see exactly like file-read NULLs. pandas DataFrames work too; use nullable
dtypes (``Int64`` etc.) where NULLs matter, since plain numpy-backed int
columns cannot represent NULL. Lists of dicts are converted to a
DataFrame, which preserves None as NULL in object columns.
"""

_PLACEHOLDER_PREFIX = "_tyr_bulk_"


def placeholder_name(dataset: str) -> str:
    """The transient table name a bulk dataset's staging SQL references."""
    return rf"{_PLACEHOLDER_PREFIX}{dataset}"


def _to_relation(rows):
    """Normalise the accepted bulk row transports to something
    duckdb.register accepts (pyarrow Table or pandas DataFrame)."""
    import pandas as pd

    try:
        import pyarrow as pa

        if isinstance(rows, pa.Table):
            return rows
        if isinstance(rows, pa.RecordBatch):
            return pa.Table.from_batches([rows])
    except ImportError:
        pass
    if isinstance(rows, pd.DataFrame):
        return rows
    if isinstance(rows, (list, tuple)):
        if rows and isinstance(rows[0], dict):
            return pd.DataFrame(list(rows))
        raise TypeError(
            "bulk rows as a sequence must be a sequence of dicts "
            f"(column: value); got element type {type(rows[0]).__name__}")
    raise TypeError(
        "bulk rows must be a pyarrow Table, a pandas DataFrame, or a "
        f"list of dicts -- got {type(rows).__name__}")


def register_bulk_sources(con, schema):
    """Materialize every bulk dataset of a schema (a Source, or a Staging
    schema whose .source is the Source) as its transient placeholder table.
    ``con`` is the RAW duckdb connection the build runs on. Returns the list
    of created table names for unregister_bulk_sources. Do not call twice
    for the same schema without unregistering -- CREATE TABLE would raise
    on the existing name."""
    source = getattr(schema, "source", schema)
    created = []
    for dataset, rows in (getattr(source, "bulk_sources", None) or {}).items():
        name = placeholder_name(dataset)
        rel = rf"__tyr_bulk_src_{dataset}"
        con.register(rel, _to_relation(rows))
        try:
            con.execute(f'CREATE TABLE {name} AS SELECT * FROM "{rel}"')
        finally:
            try:
                con.unregister(rel)
            except Exception:
                pass
        created.append(name)
    return created


def unregister_bulk_sources(con, created):
    """Drop the transient placeholder tables previously created by
    register_bulk_sources. Errors per table are swallowed -- this runs in
    a finally block and must not mask a build failure."""
    for name in created or []:
        try:
            con.execute(f"DROP TABLE IF EXISTS {name}")
        except Exception:
            pass
