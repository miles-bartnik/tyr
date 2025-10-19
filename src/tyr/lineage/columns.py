"""
Column intro
"""

from ..lineage import core as lineage
from ..lineage import values as lineage_values
from ..lineage import dataframes as lineage_dataframes
from ..lineage import functions as lineage_functions
import networkx as nx
import pandas as pd
import copy


class Select(lineage._Column):

    """
    Select creates a new column object handle from an existing column object

    :param source: Source of the selection
    :type source: lineage._Column
    :param alias: Alias to rename column to in new context - default_value [None]
    :type alias: str
    :param var_type: Variable type to determine which validation checks are run by default - default_value [None]
    :type var_type: str
    :param on_null: Behaviour when null records are encountered - default_value ["PASS"]
    :type on_null: str
    :param is_primary_key: If the column forms part of the primary key - default_value [False]
    :type is_primary_key: bool
    :param is_event_time: If the column is the event time - default_value [False]
    :type is_event_time: bool
    :param macro_group: Used to group multiple pre-fabricated lineage objects into the same custom node collection - default value [""]
    :type macro_group: str
    """

    def __init__(
        self,
        source,
        alias: str = None,
        var_type: str = None,
        on_null: str = None,
        is_primary_key: bool = False,
        is_event_time: bool = False,
        macro_group: str = "",
    ) -> None:
        if not any(
            [
                isinstance(source, lineage._Column),
                isinstance(source, lineage_dataframes.DataFrameColumn),
                isinstance(source, lineage_dataframes.LambdaOutput),
                isinstance(source, WildCard),
            ]
        ):
            print(source.__dict__)
            raise ValueError(
                "source must be lineage._Column or lineage.staging.StagingColumn"
            )

        if alias:
            name = alias
        else:
            name = source.name

        if not var_type:
            var_type = source.var_type

        if not on_null:
            on_null = source.on_null

        if not is_primary_key:
            is_primary_key = source.is_primary_key

        if not is_event_time:
            is_event_time = source.is_event_time

        super().__init__(
            source=source,
            name=name,
            data_type=source.data_type,
            var_type=var_type,
            on_null=on_null,
            is_primary_key=is_primary_key,
            is_event_time=is_event_time,
            macro_group=macro_group,
        )

        try:
            self.source_table = source.current_table
            self.current_table = source.current_table
        except:
            print(self.__dict__)

    def __deepcopy__(self, memodict={}):
        return Select(source=self.source, alias=self.name)

    def __add__(self, other):
        if (self.data_type in [lineage_values.Datatype("VARCHAR")]) and (
            other.data_type in [lineage_values.Datatype("VARCHAR")]
        ):
            return lineage_functions.string.Concatenate([self, other])

        elif (
            self.data_type
            in [lineage_values.Datatype("INTEGER"), lineage_values.Datatype("FLOAT")]
        ) and (
            other.data_type
            in [lineage_values.Datatype("INTEGER"), lineage_values.Datatype("FLOAT")]
        ):
            return lineage_functions.math.Add(self, other)

        else:
            raise SyntaxError(
                rf"Native addition not supported between {self.data_type.value} and {other.data_type.value}. Use full lineage.function syntax"
            )

    def __rmul__(self, other):
        if (
            self.data_type
            in [lineage_values.Datatype("INTEGER"), lineage_values.Datatype("FLOAT")]
        ) and (
            other.data_type
            in [lineage_values.Datatype("INTEGER"), lineage_values.Datatype("FLOAT")]
        ):
            return lineage_functions.math.Multiply(self, other)

        else:
            raise SyntaxError(
                rf"Native multiplication not supported between {self.data_type.value} and {other.data_type.value}. Use full lineage.function syntax"
            )

    def __sub__(self, other):
        if (
            self.data_type
            in [lineage_values.Datatype("INTEGER"), lineage_values.Datatype("FLOAT")]
        ) and (
            other.data_type
            in [lineage_values.Datatype("INTEGER"), lineage_values.Datatype("FLOAT")]
        ):
            return lineage_functions.math.Subtract(self, other)

        else:
            raise SyntaxError(
                rf"Native subtraction not supported between {self.data_type.value} and {other.data_type.value}. Use full lineage.function syntax"
            )

    def __truediv__(self, other):
        if (
            self.data_type
            in [lineage_values.Datatype("INTEGER"), lineage_values.Datatype("FLOAT")]
        ) and (
            other.data_type
            in [lineage_values.Datatype("INTEGER"), lineage_values.Datatype("FLOAT")]
        ):
            return lineage_functions.math.Divide(self, other)

        else:
            raise SyntaxError(
                rf"Native division not supported between {self.data_type.value} and {other.data_type.value}. Use full lineage.function syntax"
            )

    # def id(self):
    #     if "current_table" in dir(self):
    #         if self.current_table:
    #             return rf"{self.current_table.id()}.{self.name}"
    #         else:
    #             pass
    #
    #     if "source_table" in dir(self):
    #         if self.source_table:
    #             return rf"{self.source_table.id()}.{self.name}"
    #         else:
    #             pass
    #
    #     return self.name


class Core(lineage._Column):

    """
    Core is the required object for columns within tables.
    Select cannot be used in this context.
    The behaviours between Core and Select are distinct.

    :param source: Source of the selection
    :type source: lineage._Column|lineage._Function|lineage._Expression
    :param name: Alias to rename column to in new context - default_value [None]
    :type name: str
    :param var_type: Variable type to determine which validation checks are run by default - default_value [None]
    :type var_type: str
    :param on_null: Behaviour when null records are encountered - default_value ["PASS"]
    :type on_null: str
    :param is_primary_key: If the column forms part of the primary key - default_value [False]
    :type is_primary_key: bool
    :param is_event_time: If the column is the event time - default_value [False]
    :type is_event_time: bool
    :param macro_group: Used to group multiple pre-fabricated lineage objects into the same custom node collection - default value [""]
    :type macro_group: str
    """

    def __init__(
        self,
        source,
        name,
        on_null="PASS",
        is_event_time=False,
        is_primary_key=False,
        var_type: str = None,
        macro_group: str = "",
    ) -> None:
        if type(source).__bases__[0] not in [
            lineage._Value,
            lineage._Function,
            lineage._Expression,
        ] and type(source) not in [lineage.CaseWhen, Select]:
            raise ValueError(rf"Cannot create Core from {str(type(source))}")

        if not var_type:
            var_type = source.var_type

        super().__init__(
            source,
            name,
            data_type=source.data_type,
            var_type=var_type,
            on_null=on_null,
            is_event_time=is_event_time,
            is_primary_key=is_primary_key,
            macro_group=macro_group,
        )

    def __deepcopy__(self, memodict={}):
        return Core(
            source=self.source,
            name=self.name,
            var_type=self.var_type,
            is_primary_key=self.is_primary_key,
            is_event_time=self.is_event_time,
        )


class Record(lineage._Blank):

    """
    Record creates a blank column for reading data into a table from records within sql
    It is required as records do not have a source.

    :param name: Alias to rename column to in new context - default_value [None]
    :type name: str
    :param var_type: Variable type to determine which validation checks are run by default - default_value [None]
    :type var_type: str
    :param on_null: Behaviour when null records are encountered - default_value ["PASS"]
    :type on_null: str
    :param is_primary_key: If the column forms part of the primary key - default_value [False]
    :type is_primary_key: bool
    :param is_event_time: If the column is the event time - default_value [False]
    :type is_event_time: bool
    :param macro_group: Used to group multiple pre-fabricated lineage objects into the same custom node collection - default value [""]
    :type macro_group: str
    """

    def __init__(
        self,
        name,
        data_type: lineage_values.Datatype,
        var_type: str = None,
        macro_group: str = None,
        on_null: str = "PASS",
        is_primary_key: bool = False,
        is_event_time: bool = False,
    ):
        super().__init__(
            name=name,
            data_type=data_type,
            var_type=var_type,
            macro_group=macro_group,
            on_null=on_null,
            is_event_time=is_event_time,
            is_primary_key=is_primary_key,
        )


class WildCard(lineage._Column):

    """
    WildCard selects all columns from associated table.
    Use of this class breaks lineage. Only use where appropriate.
    Recommended alternative is lineage.macros.columns.select_all()

    :param macro_group: Used to group multiple pre-fabricated lineage objects into the same custom node collection - default value [""]
    :type macro_group: str


    **table_1**

    +-------+----------+-------+
    | index | event_ts | value |
    +=======+==========+=======+
    |   0   | 12:53:03 |  23   |
    +-------+----------+-------+
    |   1   | 12:54:06 |  46   |
    +-------+----------+-------+
    |   2   | 12:55:23 |  52   |
    +-------+----------+-------+

    **Usage**
    ::
        tables.Core(
            name="test_wildcard",
            source=tables.Select(table_1),
            columns=core.ColumnList(
                [
                    columns.WildCard()
                ]
            )
        )

    """

    def __init__(self, macro_group: str = ""):
        source = lineage_values.WildCard()

        super().__init__(
            name=source.name,
            source=source,
            var_type=source.var_type,
            data_type=source.data_type,
            on_null="PASS",
            is_primary_key=False,
            is_event_time=False,
            macro_group=macro_group,
        )
