from ..beeswax import lineage
import units
import time


def selector(item):
    # MACRO

    if type(item).__bases__[0] is lineage.macros.core.Macro:
        function_name = "macros_macro"

    # SOURCE

    elif type(item).__bases__[0] is lineage.core._SourceColumn:
        function_name = "source_column"

    elif type(item).__bases__[0] is lineage.core._SourceFile:
        function_name = "source_file"

    elif type(item).__bases__[0] is lineage.core._BlankColumn:
        function_name = "columns_blank"

    # COLUMNS

    elif type(item).__bases__[0] is lineage.core._Column:
        if type(item) is lineage.columns.Select:
            function_name = "column_select"

        elif type(item) is lineage.columns.Core:
            function_name = "columns_core"

        elif type(item) is lineage.columns.Blank:
            function_name = "columns_blank"

    # VALUES

    elif type(item).__bases__[0] is lineage.core._Value:
        if type(item) is lineage.values.Varchar:
            function_name = "values_varchar"

        elif type(item) is lineage.values.Integer:
            function_name = "values_integer"

        elif type(item) is lineage.values.Float:
            function_name = "values_float"

        elif type(item) is lineage.values.Timestamp:
            function_name = "values_timestamp"

        elif type(item) is lineage.values.Date:
            function_name = "values_date"

        elif type(item) is lineage.values.WildCard:
            function_name = "values_wildcard"

        elif type(item) is lineage.values.Subquery:
            function_name = "values_subquery"

        elif type(item) is lineage.values.Null:
            function_name = "values_null"

        elif type(item) is lineage.values.Interval:
            function_name = "values_interval"

        elif type(item) is lineage.values.List:
            function_name = "values_list"

        elif type(item) is lineage.values.GeoCoordinate:
            function_name = "values_list"

        elif type(item) is lineage.values.Datatype:
            function_name = "values_datatype"

        elif type(item) is lineage.values.Tuple:
            function_name = "values_tuple"

        elif type(item) is lineage.values.Boolean:
            function_name = "values_boolean"

        elif type(item) is lineage.values.Struct:
            function_name = "values_struct"

    # UNITS

    elif type(item) is units.core.Unit:
        function_name = "core_unit"

    # TABLES

    elif type(item).__bases__[0] is lineage.core._Table:
        if type(item) is lineage.tables.Core:
            function_name = "tables_core"

        elif type(item) is lineage.tables.Select:
            function_name = "tables_select"

        elif type(item) is lineage.tables.Subquery:
            function_name = "tables_subquery"

        elif type(item) is lineage.tables.Temp:
            function_name = "tables_temp"

        elif type(item) is lineage.tables.FromRecords:
            function_name = "tables_from_records"

        elif type(item) is lineage.tables.Union:
            function_name = "unions_union"

    # FUNCTIONS

    elif type(item).__bases__[0] is lineage.core._Function:
        if type(item) not in [
            lineage.functions.data_type.ToInterval,
            lineage.functions.array.ListExtract,
            lineage.functions.json.JSONExtract,
            lineage.functions.window.RowNumber,
            lineage.functions.union.UnionColumn,
            lineage.functions.string.StringExtract,
        ]:
            function_name = "core_function"
        elif type(item) is lineage.functions.data_type.ToInterval:
            function_name = "functions_to_interval"
        elif type(item) is lineage.functions.array.ListExtract:
            function_name = "functions_list_extract"
        elif type(item) is lineage.functions.json.JSONExtract:
            function_name = "functions_json_extract"
        elif type(item) is lineage.functions.window.RowNumber:
            function_name = "functions_row_number"
        elif type(item) is lineage.functions.union.UnionColumn:
            function_name = "unions_union_column"
        elif type(item) is lineage.functions.string.StringExtract:
            function_name = "functions_list_extract"

    # EXPRESSIONS
    elif type(item).__bases__[0] is lineage.core._Expression:
        function_name = "core_expression"

    # OPERATORS
    elif type(item).__bases__[0] is lineage.core._Operator:
        function_name = "core_operator"

    # JOINS
    elif type(item) is lineage.joins.Join:
        function_name = "joins_join"

    elif type(item) is lineage.joins.CompoundJoin:
        function_name = "joins_compound_join"

    # CORE
    elif type(item) is lineage.core.CaseWhen:
        function_name = "core_case_when"

    elif type(item) is lineage.core.Condition:
        function_name = "core_condition"

    elif type(item) is lineage.core.OrderBy:
        function_name = "core_order_by"

    elif (
        type(item).__bases__[0] is lineage.schema.SchemaSettings
        or type(item) is lineage.schema.SchemaSettings
    ):
        function_name = "schema_settings"

    elif type(item) is lineage.core.Record:
        function_name = "core_record"

    elif type(item) is lineage.core.RecordList:
        function_name = "core_record_list"

    elif type(item) is lineage.core.RecordGenerator:
        function_name = "core_record_generator"

    # TRANSFORMATIONS
    elif type(item) is lineage.transformations.Limit:
        function_name = "transformations_limit"

    elif type(item) is lineage.transformations.ReadCSV:
        function_name = "core_transformation"

    elif type(item) is lineage.transformations.ReadGeoJson:
        function_name = "core_transformation"

    # DATAFRAMES
    elif type(item) is lineage.dataframes.DataFrame:
        function = "dataframes_data_frame"

    elif type(item) is lineage.dataframes.DataFrameColumn:
        function = "dataframes_data_frame_column"

    elif type(item) is lineage.dataframes.LambdaOutput:
        function = "dataframes_lambda_output"

    else:
        raise ValueError("COULD NOT PARSE - ", type(item))

    return function_name
