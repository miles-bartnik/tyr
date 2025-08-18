import units
import time
from . import duckdb as duckdb_syntax

syntax_dict = {"duckdb": duckdb_syntax}


def selector(item):
    function_name = None

    # SOURCE

    if "lineage.schema.source.SourceFile" in str(type(item)):
        function_name = "source_file"

    elif "lineage.core._BlankColumn" in str(type(item).__bases__[0]):
        function_name = "columns_blank"

    # COLUMNS

    elif "lineage.core._Column" in str(type(item).__bases__[0]):
        if "lineage.columns.Select" in str(type(item)):
            function_name = "columns_select"

        elif "lineage.columns.Core" in str(type(item)):
            function_name = "columns_core"

        elif "lineage.columns.Blank" in str(type(item)):
            function_name = "columns_blank"

        elif "lineage.columns.WildCard" in str(type(item)):
            function_name = "columns_wild_card"

        elif "lineage.columns.Core" in str(type(item).__bases__[0]):
            function_name = "columns_core"

        elif "lineage.core._Column" in str(type(item).__bases__[0]):
            function_name = "columns_core"

    # VALUES

    elif "lineage.core._Value" in str(type(item).__bases__[0]):
        if "lineage.values.Varchar" in str(type(item)):
            function_name = "values_varchar"

        elif "lineage.values.Integer" in str(type(item)):
            function_name = "values_integer"

        elif "lineage.values.Float" in str(type(item)):
            function_name = "values_float"

        elif "lineage.values.Timestamp" in str(type(item)):
            function_name = "values_timestamp"

        elif "lineage.values.Date" in str(type(item)):
            function_name = "values_date"

        elif "lineage.values.WildCard" in str(type(item)):
            function_name = "values_wildcard"

        elif "lineage.values.Subquery" in str(type(item)):
            function_name = "values_subquery"

        elif "lineage.values.Null" in str(type(item)):
            function_name = "values_null"

        elif "lineage.values.Interval" in str(type(item)):
            function_name = "values_interval"

        elif "lineage.values.List" in str(type(item)):
            function_name = "values_list"

        elif "lineage.values.GeoCoordinate" in str(type(item)):
            function_name = "values_list"

        elif "lineage.values.Datatype" in str(type(item)):
            function_name = "values_datatype"

        elif "lineage.values.Tuple" in str(type(item)):
            function_name = "values_tuple"

        elif "lineage.values.Boolean" in str(type(item)):
            function_name = "values_boolean"

        elif "lineage.values.Struct" in str(type(item)):
            function_name = "values_struct"

    # UNITS

    elif str(type(item)) is units.core.Unit:
        function_name = "core_unit"

    # TABLES

    elif "lineage.core._Table" in str(type(item).__bases__[0]):
        if "lineage.tables.Core" in str(type(item)):
            function_name = "tables_core"

        elif "lineage.tables.Select" in str(type(item)):
            function_name = "tables_select"

        elif "lineage.tables.Subquery" in str(type(item)):
            function_name = "tables_subquery"

        elif "lineage.tables.Temp" in str(type(item)):
            function_name = "tables_temp"

        elif "lineage.tables.FromRecords" in str(type(item)):
            function_name = "tables_from_records"

        elif "lineage.tables.Union" in str(type(item)):
            function_name = "unions_union"

        elif "lineage.tables.Core" in str(type(item).__bases__[0]):
            function_name = "tables_core"

        elif "lineage.core._Table" in str(type(item).__bases__[0]):
            function_name = "tables_core"

    # FUNCTIONS

    elif "lineage.core._Function" in str(type(item).__bases__[0]):
        if not any(
            [
                bh_type in str(type(item))
                for bh_type in [
                    "lineage.functions.data_type.ToInterval",
                    "lineage.functions.array.ListExtract",
                    "lineage.functions.json.JSONExtract",
                    "lineage.functions.window.RowNumber",
                    "lineage.functions.union.UnionColumn",
                    "lineage.functions.string.StringExtract",
                    "lineage.functions.utility.SourceWildToStagingColumn",
                ]
            ]
        ):
            function_name = "core_function"
        elif "lineage.functions.data_type.ToInterval" in str(type(item)):
            function_name = "functions_to_interval"
        elif "lineage.functions.array.ListExtract" in str(type(item)):
            function_name = "functions_list_extract"
        elif "lineage.functions.json.JSONExtract" in str(type(item)):
            function_name = "functions_json_extract"
        elif "lineage.functions.window.RowNumber" in str(type(item)):
            function_name = "functions_row_number"
        elif "lineage.functions.union.UnionColumn" in str(type(item)):
            function_name = "unions_union_column"
        elif "lineage.functions.string.StringExtract" in str(type(item)):
            function_name = "functions_list_extract"
        elif "lineage.functions.utility.SourceWildToStagingColumn" in str(type(item)):
            function_name = "functions_source_wild_to_staging"

    # EXPRESSIONS
    elif "lineage.core._Expression" in str(type(item).__bases__[0]):
        function_name = "core_expression"

    # OPERATORS
    elif "lineage.core._Operator" in str(type(item).__bases__[0]):
        function_name = "core_operator"

    # JOINS
    elif "lineage.joins.Join" in str(type(item)):
        function_name = "joins_join"

    elif "lineage.joins.CompoundJoin" in str(type(item)):
        function_name = "joins_compound_join"

    # CORE
    elif "lineage.core.CaseWhen" in str(type(item)):
        function_name = "core_case_when"

    elif "lineage.core.Condition" in str(type(item)):
        function_name = "core_condition"

    elif "lineage.core.OrderBy" in str(type(item)):
        function_name = "core_order_by"

    elif "lineage.schema.core._SchemaSettings" in str(type(item).__bases__[0]):
        function_name = "schema_settings"

    elif "lineage.schema.core._Schema" in str(type(item).__bases__[0]):
        function_name = "core_schema"

    elif "lineage.core.RecordList" in str(type(item)):
        function_name = "core_record_list"

    elif "lineage.core.RecordGenerator" in str(type(item)):
        function_name = "core_record_generator"

    elif "lineage.core.Record" in str(type(item)):
        function_name = "core_record"

    # TRANSFORMATIONS
    elif "lineage.transformations.Limit" in str(type(item)):
        function_name = "transformations_limit"

    elif "lineage.transformations.ReadCSV" in str(type(item)):
        function_name = "core_transformation"

    elif "lineage.transformations.ReadGeoJson" in str(type(item)):
        function_name = "core_transformation"

    # DATAFRAMES
    elif "lineage.dataframes.DataFrame" in str(type(item)):
        function_name = "dataframes_data_frame"

    elif "lineage.dataframes.DataFrameColumn" in str(type(item)):
        function_name = "dataframes_data_frame_column"

    elif "lineage.dataframes.LambdaOutput" in str(type(item)):
        function_name = "dataframes_lambda_output"

    else:
        raise ValueError("COULD NOT PARSE - ", str(type(item)))

    if function_name:
        return function_name
    else:
        raise ValueError("COULD NOT PARSE - ", str(type(item)))


class Syntax:
    def __init__(self, name):
        self.name = name
        self.syntax = syntax_dict[name]

    def item_to_sql(self, item):
        return self.syntax.__dict__[selector(item)](item)


class DuckDB(Syntax):
    def __init__(self):
        super().__init__(name="duckdb")
