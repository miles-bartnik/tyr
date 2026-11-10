"""lock_source: a file_metadata flag that takes a dataset OUT of the source
refresh (owner call 2026-11-10).

lock_source=True means the SOURCE stage skips the dataset -- no staging
table is built from the file, so a staging->project carry leaves the
existing project table untouched (user edits survive a re-source). Absent
column (older TSVs), empty values, and anything but an explicit
true/1/yes mean unlocked -- the dataset stages from its file as usual.
"""

import pandas as pd

from tyr.lineage.schema import source as source_schema


_COLUMNS = ["schema", "dataset", "file_regex", "delim", "distinct",
            "required", "lock_source"]


def _rows(*lock_values):
    return pd.DataFrame(
        [{
            "schema": "staging",
            "dataset": f"ds_{i}",
            "file_regex": f"C:/tmp/ds_{i}.csv",
            "delim": "c",
            "distinct": "False",
            "required": "True",
            "lock_source": lock,
        } for i, lock in enumerate(lock_values)]
    )


def _source(lock_values):
    return source_schema.Source(
        settings=source_schema.SourceSettings(
            file_metadata={
                f"ds_{i}": source_schema.FileMetadata(row)
                for i, row in _rows(*lock_values).iterrows()
            },
            expected_column_metadata={f"ds_{i}": {}
                                      for i in range(len(lock_values))},
            extensions=[],
        )
    )


def _names(src):
    return [t.name for t in src.tables.list_tables()]


def test_lock_source_true_skips_the_dataset():
    assert _names(_source(("True", "False"))) == ["ds_1"]


def test_lock_source_variants():
    for truthy in ("true", "1", "yes", "TRUE", " tRue "):
        assert _names(_source((truthy,))) == [], f"{truthy!r} must lock"
    for falsy in ("False", "0", "no", ""):
        assert _names(_source((falsy,))) == ["ds_0"], f"{falsy!r} must not lock"


def test_absent_column_means_unlocked():
    src = source_schema.Source(
        settings=source_schema.SourceSettings(
            file_metadata={
                "ds_0": source_schema.FileMetadata(
                    pd.Series({
                        "schema": "staging", "dataset": "ds_0",
                        "file_regex": "C:/tmp/ds_0.csv",
                        "delim": "c", "distinct": "False",
                    })
                ),
            },
            expected_column_metadata={"ds_0": {}},
            extensions=[],
        )
    )
    names = _names(src)
    assert names == ["ds_0"]
    locked = source_schema.FileMetadata(
        pd.Series({
            "schema": "staging", "dataset": "ds_0",
            "file_regex": "C:/tmp/ds_0.csv",
            "delim": "c", "distinct": "False",
        })
    )
    assert locked.lock_source is False
