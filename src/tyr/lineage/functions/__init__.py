import inspect

from . import (
    aggregate,
    math,
    array,
    data_type,
    datetime,
    geo,
    json,
    math,
    string,
    utility,
    window,
    union,
)
from .. import core as _lineage

# The function modules to enumerate for reference() -- one category each.
_MODULES = (
    aggregate,
    array,
    data_type,
    datetime,
    geo,
    json,
    math,
    string,
    union,
    utility,
    window,
)


def reference():
    """Every function class across the tyr function modules as
    ``{category, name, sql, params, doc}`` dicts, for building a SQL reference in a
    consumer (e.g. a query editor). Nothing is instantiated -- ``sql`` comes from
    :meth:`tyr.lineage.core._Function.constructor_sql`. Sorted by (category, sql)."""
    out = []
    for module in _MODULES:
        category = module.__name__.rsplit(".", 1)[-1]
        for _name, obj in inspect.getmembers(module, inspect.isclass):
            if (
                issubclass(obj, _lineage._Function)
                and obj is not _lineage._Function
                and obj.__module__ == module.__name__
            ):
                params = [
                    p
                    for p in inspect.signature(obj.__init__).parameters
                    if p != "self"
                ]
                # the class's OWN docstring (not the inherited _Function base one)
                own_doc = (obj.__dict__.get("__doc__") or "").strip()
                # the appended 'DuckDB: <url>' documentation link, if any
                url = ""
                for line in own_doc.split("\n"):
                    line = line.strip()
                    if line.startswith("DuckDB:"):
                        url = line[len("DuckDB:"):].strip()
                        break
                out.append(
                    {
                        "category": category,
                        "name": obj.__name__,
                        "sql": obj.constructor_sql(),
                        "params": params,
                        "doc": own_doc.split("\n")[0],
                        "url": url,
                    }
                )
    return sorted(out, key=lambda r: (r["category"], r["sql"]))
