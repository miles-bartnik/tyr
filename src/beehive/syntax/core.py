from ..beeswax import lineage
import units


def selector(item):
    # SOURCE

    if type(item).__bases__[0] is lineage.core._SourceColumn:
        function_name = "source_column"

    elif type(item).__bases__[0] is lineage.core._SourceFile:
        function_name = "source_file"

    elif type(item).__bases__[0] is lineage.core._BlankColumn:
        function_name = "columns_blank"

    elif type(item) is lineage.core.DataFrame:
        function_name = "core_data_frame"

    # COLUMNS

    elif type(item).__bases__[0] is lineage.core._Column:
        if type(item) is lineage.columns.Select:
            function_name = "column_select"

        elif type(item) is lineage.columns.Expand:
            function_name = "column_expand"

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

        elif type(item) is lineage.values.WildCard:
            function_name = "values_wildcard"

        elif type(item) is lineage.values.Subquery:
            function_name = "values_subquery"

        elif type(item) is lineage.values.Null:
            function_name = "values_null"

        elif type(item) is lineage.values.Interval:
            function_name = "values_interval"

        elif type(item) is lineage.values.Limit:
            function_name = "values_limit"

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

    # FUNCTIONS

    elif type(item).__bases__[0] is lineage.core._Function:
        if type(item) not in [
            lineage.functions.ToInterval,
            lineage.functions.ListExtract,
            lineage.functions.JSONExtract,
            lineage.functions.RowNumber,
        ]:
            function_name = "core_function"
        elif type(item) is lineage.functions.ToInterval:
            function_name = "functions_to_interval"
        elif type(item) is lineage.functions.ListExtract:
            function_name = "functions_list_extract"
        elif type(item) is lineage.functions.JSONExtract:
            function_name = "functions_json_extract"
        elif type(item) is lineage.functions.RowNumber:
            function_name = "functions_row_number"
    # AGGREGATES

    elif type(item).__bases__[0] is lineage.core._Aggregate:
        function_name = "core_aggregate"

    # EXPRESSIONS
    elif type(item).__bases__[0] is lineage.core._Expression:
        function_name = "core_expression"

    # OPERATORS
    elif type(item).__bases__[0] is lineage.core._Operator:
        function_name = "core_operator"

    # COMBINATIONS
    elif type(item) is lineage.combinations.Join:
        function_name = "combinations_join"

    elif type(item) is lineage.combinations.JoinList:
        function_name = "combinations_join_list"

    elif type(item) is lineage.combinations.Union:
        function_name = "combinations_union"

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

    else:
        raise ValueError("COULD NOT PARSE - ", type(item))

    return function_name
