from ..lineage import core as lineage
from ..lineage import values as lineage_values
from ..lineage import dataframes as lineage_dataframes
import networkx as nx
import pandas as pd
import copy


class Select(lineage._Column):
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
                isinstance(source, Blank),
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


class Blank(lineage._BlankColumn):
    def __init__(
        self,
        name: str = None,
        var_type: str = None,
        data_type: lineage_values.Datatype = None,
        on_null: str = None,
        is_primary_key: bool = False,
        is_event_time: bool = False,
        unit: lineage.units.core.Unit = lineage.units.core.Unit(),
        macro_group: str = "",
    ):
        self.name = name
        self.current_table = None

        super().__init__(
            name=name,
            var_type=var_type,
            data_type=data_type,
            on_null=on_null,
            is_primary_key=is_primary_key,
            is_event_time=is_event_time,
            unit=unit,
            macro_group=macro_group,
        )

    def __deepcopy__(self, memodict={}):
        return Blank(
            name=self.name,
            var_type=self.var_type,
            data_type=self.data_type,
            on_null=self.on_null,
            is_primary_key=self.is_primary_key,
            is_event_time=self.is_event_time,
            unit=self.unit,
        )


class WildCard(lineage._Column):
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
