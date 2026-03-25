"""
Units module
"""

import pandas as pd
from typing import List
import re
import math
import duckdb
import warnings
from datetime import datetime
from . import supported
import unicodedata
import types


currencies = [
    "\$",
    "\¢",
    "\£",
    "\¤",
    "\¥",
    "\֏",
    "\৲",
    "\৳",
    "\৻",
    "\૱",
    "\௹",
    "\฿",
    "\៛",
    "\₠",
    "\₡",
    "\₢",
    "\₣",
    "\₤",
    "\₥",
    "\₦",
    "\₧",
    "\₨",
    "\₩",
    "\₪",
    "\₫",
    "\€",
    "\₭",
    "\₮",
    "\₯",
    "\₰",
    "\₱",
    "\₲",
    "\₳",
    "\₴",
    "\₵",
    "\₶",
    "\₷",
    "\₸",
    "\₺",
    "\₻",
    "\₼",
    "\₽",
    "\₾",
    "\₿",
    "\﹩",
    "\＄",
    "\￠",
    "\￡",
    "\￥",
    "\￦",
]

_unit_regex = (
    rf"(?P<symbol>[′°A-Za-z{''.join(currencies)}]+?)\^(?P<exponent>-?[\.\d]+?)"
)

_supported_units = pd.DataFrame.from_records(
    [
        {"type": quantity[0]} | unit
        for quantity in [
            x for x in supported.__dict__.items() if isinstance(x[1], types.ModuleType)
        ]
        for unit in quantity[1].units
    ]
)

_supported_units["root_unit_symbol"] = _supported_units.apply(
    lambda row: (
        re.match(_unit_regex, row["root_unit"]).groupdict()["symbol"]
        if row["root_unit"]
        else ""
    ),
    axis=1,
)

_SI_prefixes = pd.DataFrame.from_records(
    [
        {"name": "queta", "symbol": "Q", "exponent": 30},
        {"name": "ronna", "symbol": "R", "exponent": 27},
        {"name": "yotta", "symbol": "Y", "exponent": 24},
        {"name": "zetta", "symbol": "Z", "exponent": 21},
        {"name": "exa", "symbol": "E", "exponent": 18},
        {"name": "peta", "symbol": "P", "exponent": 15},
        {"name": "tera", "symbol": "T", "exponent": 12},
        {"name": "giga", "symbol": "G", "exponent": 9},
        {"name": "mega", "symbol": "M", "exponent": 6},
        {"name": "kilo", "symbol": "k", "exponent": 3},
        {"name": "hecta", "symbol": "h", "exponent": 2},
        {"name": "deca", "symbol": "da", "exponent": 1},
        {"name": "", "symbol": "", "exponent": 0},
        {"name": "quecto", "symbol": "q", "exponent": -30},
        {"name": "ronto", "symbol": "r", "exponent": -27},
        {"name": "yocto", "symbol": "y", "exponent": -24},
        {"name": "zepto", "symbol": "z", "exponent": -21},
        {"name": "atto", "symbol": "a", "exponent": -18},
        {"name": "femto", "symbol": "f", "exponent": -15},
        {"name": "pico", "symbol": "p", "exponent": -12},
        {"name": "nano", "symbol": "n", "exponent": -9},
        {"name": "micro", "symbol": "μ", "exponent": -6},
        {"name": "milli", "symbol": "m", "exponent": -3},
        {"name": "centi", "symbol": "c", "exponent": -2},
        {"name": "deci", "symbol": "d", "exponent": -1},
    ]
)

_harvestman = duckdb.connect(":memory:")


_shift_factors = pd.DataFrame.from_records(
    [
        {
            "source_unit": "°C^1",
            "target_unit": "K^1",
            "pre_shift_factor": 273.15,
            "conversion_factor": 1.0,
            "post_shift_factor": 0.0,
        },
        {
            "source_unit": "°F^1",
            "target_unit": "K^1",
            "pre_shift_factor": 459.67,
            "conversion_factor": 5 / 9,
            "post_shift_factor": 0.0,
        },
        {
            "source_unit": "°R^1",
            "target_unit": "K^1",
            "pre_shift_factor": 0.0,
            "conversion_factor": 5 / 9,
            "post_shift_factor": 0.0,
        },
    ]
)

for target_unit in ["°C^1", "°F^1", "°R^1"]:
    _shift_factors = pd.concat(
        [
            _shift_factors,
            pd.DataFrame.from_records(
                [
                    {
                        "source_unit": "K^1",
                        "target_unit": target_unit,
                        "pre_shift_factor": -_shift_factors[
                            (_shift_factors["source_unit"] == target_unit)
                            & (_shift_factors["target_unit"] == "K^1")
                        ]["post_shift_factor"].iloc[0],
                        "conversion_factor": _shift_factors[
                            (_shift_factors["source_unit"] == target_unit)
                            & (_shift_factors["target_unit"] == "K^1")
                        ]["conversion_factor"].iloc[0]
                        ** -1,
                        "post_shift_factor": -_shift_factors[
                            (_shift_factors["source_unit"] == target_unit)
                            & (_shift_factors["target_unit"] == "K^1")
                        ]["pre_shift_factor"].iloc[0],
                    }
                ]
            ),
        ]
    )


_combined_units = _harvestman.execute(
    rf"""
WITH supported_units AS (
        SELECT *, 
               CASE 
                    WHEN (LOG(conversion_factor)-FLOOR(LOG(conversion_factor))) BETWEEN -POW(10, -10) AND POW(10, -10) 
                    THEN TRUE 
                    ELSE FALSE 
               END AS conversion_factor_check
        FROM _supported_units
        ),
    si_units AS (
        SELECT * FROM supported_units 
        WHERE conversion_factor_check IS TRUE
            AND type NOT IN ('currency')
        ),
    non_si_units AS (
        SELECT * FROM supported_units 
        WHERE NOT conversion_factor_check IS TRUE
            OR type IN ('currency')
    ),
    si_prefixes AS (
        SELECT name AS prefix_name, symbol AS prefix_symbol, exponent AS prefix_exponent FROM _SI_prefixes
        ),
    combined_si_units AS (
        SELECT si_units.name,
               si_units.symbol,
               si_units.type,
               si_units.root_unit,
               si_units.root_unit_symbol,
               si_units.conversion_factor,
               si_prefixes.prefix_name,
               si_prefixes.prefix_exponent,
               si_prefixes.prefix_symbol 
        FROM si_units CROSS JOIN si_prefixes
        ),
    combined_non_si_units AS ( 
        SELECT non_si_units.name,
               non_si_units.symbol,
               non_si_units.type,
               non_si_units.root_unit,
               non_si_units.root_unit_symbol,
               non_si_units.conversion_factor,
               si_prefixes.prefix_name,
               si_prefixes.prefix_exponent,
               si_prefixes.prefix_symbol 
        FROM non_si_units 
        CROSS JOIN 
        (SELECT * FROM si_prefixes WHERE prefix_name='') AS si_prefixes
        ),
    combined_units AS (
        SELECT name,
               symbol,
               root_unit,
               type,
               root_unit_symbol,
               conversion_factor,
               prefix_name,
               prefix_exponent, 
               prefix_symbol, 
               TRUE AS is_si,
        FROM combined_si_units 
        UNION 
        SELECT name,
               symbol,
               root_unit,
               type,
               root_unit_symbol,
               conversion_factor,
               prefix_name,
               prefix_exponent, 
               prefix_symbol, 
               FALSE AS is_si 
        FROM combined_non_si_units
        )
SELECT CONCAT(prefix_name, name) AS prefixed_unit_name,
       CONCAT(prefix_symbol, symbol) AS prefixed_unit_symbol,
       name AS unit_name,
       symbol AS unit_symbol, 
       type AS unit_type,
       root_unit AS root_unit,
       root_unit_symbol AS root_unit_symbol,
       conversion_factor AS conversion_factor,
       prefix_symbol AS prefix_symbol,
       prefix_exponent AS prefix_exponent, 
       is_si AS unit_is_si
FROM combined_units 
--LEFT JOIN _shift_factors
"""
).df()

_harvestman.close()


class Unit:
    def __init__(
        self,
        unit: str = "",
        date_vector: pd.Series = pd.Series(
            data={"date": datetime.now().strftime("%d-%m-%Y")}
        ),
        macro_group: str = "",
    ):
        self.sub_units = pd.DataFrame()
        self.date_vector = date_vector
        self.macro_group = macro_group

        # Holy fuck I need to comment here on how fucking terribly pandas handles nan
        if (unit) and (type(unit) is str):
            if unit.lower() != "nan":
                sub_units = (
                    pd.DataFrame.from_records(
                        [match.groupdict() for match in re.finditer(_unit_regex, unit)]
                    )
                    .reset_index()
                    .rename(columns={"index": "order"})
                )

                if not sub_units.empty:
                    if "ft" in sub_units.symbol.unique():
                        warnings.warn(
                            "WARNING: 'ft' defaults to femtoton. If imperial foot is required, it must be specified as 'foot'"
                        )
                    if "kt" in sub_units.symbol.unique():
                        warnings.warn(
                            "WARNING: 'kt' defaults to kiloton. If imperial knot is required, it must be specified as 'knot' or 'kn'"
                        )

                    _harvestman = duckdb.connect(":memory:")

                    self.sub_units = _harvestman.execute(
                        rf"""
                        WITH base AS (
                                SELECT sub_units.order + 1 AS sub_unit_order,
                                       sub_units.symbol AS sub_unit_symbol,
                                       CAST(sub_units.exponent AS FLOAT) AS sub_unit_exponent,
                                       _combined_units.unit_name AS non_prefixed_unit_name,
                                       _combined_units.unit_symbol AS non_prefixed_unit_symbol,
                                       _combined_units.unit_type AS unit_type,
                                       _combined_units.prefixed_unit_name AS unit_name,
                                       _combined_units.root_unit AS root_unit,
                                       _combined_units.conversion_factor AS conversion_factor,
                                       _combined_units.prefix_exponent AS prefix_exponent,
                                       _combined_units.prefix_symbol AS prefix_symbol,
                                FROM sub_units
                                LEFT JOIN _combined_units
                                ON sub_units.symbol = _combined_units.prefixed_unit_symbol
                                ),
                                ordered AS (
                                SELECT MAX(sub_unit_order) AS sub_unit_order,
                                       FIRST(sub_unit_symbol) AS sub_unit_symbol,
                                       FIRST(unit_name) AS sub_unit_name,
                                       FIRST(non_prefixed_unit_symbol) AS non_prefixed_sub_unit_symbol,
                                       FIRST(non_prefixed_unit_name) AS non_prefixed_sub_unit_name,
                                       FIRST(unit_type) AS sub_unit_type,
                                       SUM(sub_unit_exponent) AS sub_unit_exponent,
                                       FIRST(root_unit) AS root_unit,
                                       PRODUCT(conversion_factor) AS conversion_factor,
                                       SUM(prefix_exponent) AS prefix_exponent,
                                       FIRST(prefix_symbol) AS prefix_symbol
                                FROM base GROUP BY sub_unit_symbol
                                HAVING SUM(sub_unit_exponent) != 0
                                )
                        SELECT ROW_NUMBER() OVER (ORDER BY sub_unit_order ASC) AS order,
                               sub_unit_name AS unit_name,
                               sub_unit_symbol AS symbol,
                               non_prefixed_sub_unit_name AS non_prefixed_name,
                               non_prefixed_sub_unit_symbol AS non_prefixed_symbol,
                               sub_unit_type AS unit_type,
                               root_unit AS root_unit,
                               conversion_factor,
                               sub_unit_exponent AS exponent,
                               prefix_exponent,
                               prefix_symbol,
                        FROM ordered
                        """
                    ).df()

                    _harvestman.close()

        if not self.sub_units.empty:
            self.name = "".join(
                [
                    (
                        rf"{row['symbol']}^{int(row['exponent'])}"
                        if math.isclose(
                            row["exponent"],
                            math.floor(row["exponent"]),
                            rel_tol=1e10,
                            abs_tol=0,
                        )
                        else rf"{row['symbol']}^{float(row['exponent'])}"
                    )
                    for index, row in self.sub_units.sort_values(
                        ["exponent", "symbol"], ascending=False
                    ).iterrows()
                ]
            )

            self.root_unit_name = "".join(
                [
                    (
                        rf"{row['symbol']}^{int(row['exponent'])}"
                        if math.isclose(
                            row["exponent"],
                            math.floor(row["exponent"]),
                            rel_tol=1e10,
                            abs_tol=0,
                        )
                        else rf"{row['symbol']}^{float(row['exponent'])}"
                    )
                    for index, row in self.sub_units.sort_values(
                        ["exponent", "symbol"], ascending=False
                    ).iterrows()
                ]
            )

        else:
            self.name = ""

        self._node_data = {
            "label": self.name,
            "type": type(self),
            "base": type(self).__bases__[0],
            "macro_group": self.macro_group,
        }

    def reciprocal(self):
        if not self.sub_units.empty:
            return Unit(
                "".join(
                    [
                        (
                            rf"{row['symbol']}^{int(-row['exponent'])}"
                            if math.isclose(
                                -row["exponent"],
                                math.floor(-row["exponent"]),
                                rel_tol=1e10,
                                abs_tol=0,
                            )
                            else rf"{row['symbol']}^{float(-row['exponent'])}"
                        )
                        for index, row in self.sub_units.sort_values(
                            ["exponent", "symbol"], ascending=False
                        ).iterrows()
                    ]
                )
            )
        else:
            return Unit()

    def _inbound_edge_data(self):
        return {}

    def _outbound_edge_data(self):
        return {}

    def __eq__(self, other):

        if isinstance(other, Unit):

            if self.sub_units.empty:
                if other.sub_units.empty:
                    return True
                else:
                    return False

            elif other.sub_units.empty:
                if self.sub_units.empty:
                    return True
                else:
                    return False

            else:
                return self.sub_units.sort_values(
                    ["exponent", "symbol"], ascending=False
                ).equals(
                    other.sub_units.sort_values(["exponent", "symbol"], ascending=False)
                )
        else:
            return False


def get_conversion_plan(source_unit, target_unit):
    if source_unit.sub_units.empty:
        return pd.DataFrame(
            columns=[
                "source_unit",
                "target_unit",
                "unit_type",
                "direction",
                "apply_prefix",
                "apply_conversion",
                "apply_pre_shift",
                "apply_post_shift",
            ]
        )

    unit_from_sub_units = pd.DataFrame.copy(source_unit.sub_units, deep=True)
    unit_from_sub_units["direction"] = -1

    unit_to_sub_units = pd.DataFrame.copy(target_unit.sub_units, deep=True)
    unit_to_sub_units["direction"] = 1

    _conversion_steps = pd.concat([unit_from_sub_units, unit_to_sub_units], axis=0)

    conversion_plan_query = rf"""
        WITH conversion_steps AS (
            SELECT *, 
                   CONCAT(
                          _conversion_steps.non_prefixed_symbol,
                          '^',
                          CASE
                            WHEN _conversion_steps.exponent=1
                            THEN '1'
                            ELSE CAST(_conversion_steps.exponent AS VARCHAR)
                          END
                   ) AS unit
            FROM _conversion_steps
            WHERE _conversion_steps.exponent!=0
        ),
        direction_applied AS (
              SELECT
                direction,
                CASE
                 WHEN direction=1
                  THEN root_unit
                  WHEN direction=-1
                  THEN unit
                  ELSE CAST(NULL AS VARCHAR)
                END AS source_unit,
                CASE
                  WHEN direction=1
                  THEN unit
                  WHEN direction=-1
                  THEN root_unit
                  ELSE CAST(NULL AS VARCHAR)
                END AS target_unit,
                unit_type,
                prefix_symbol,
                prefix_exponent,
                -- POWER(10, -conversion_steps.direction*conversion_steps.prefix_exponent) AS apply_prefix,
                POWER(conversion_steps.conversion_factor, conversion_steps.direction) AS conversion_factor,
              FROM conversion_steps
        )
        SELECT CASE WHEN direction = -1 THEN CONCAT(COALESCE(direction_applied.prefix_symbol, ''), direction_applied.source_unit)
                    WHEN direction = 1 THEN direction_applied.source_unit END AS source_unit,
               CASE WHEN direction = 1 THEN CONCAT(COALESCE(direction_applied.prefix_symbol, ''), direction_applied.target_unit)
                    WHEN direction = -1 THEN direction_applied.target_unit END AS target_unit,
               direction_applied.unit_type AS unit_type,
              direction_applied.direction AS direction,
              POWER(10, -direction_applied.direction*direction_applied.prefix_exponent) AS apply_prefix,
              COALESCE(_shift_factors.conversion_factor, direction_applied.conversion_factor, 1) AS apply_conversion,
              COALESCE(_shift_factors.pre_shift_factor, 0) AS apply_pre_shift,
              COALESCE(_shift_factors.post_shift_factor, 0) AS apply_post_shift,
        FROM direction_applied
        LEFT JOIN _shift_factors ON direction_applied.source_unit = _shift_factors.source_unit
                                            AND
                                    direction_applied.target_unit = _shift_factors.target_unit
        ORDER BY direction_applied.direction ASC
        """

    _harvestman = duckdb.connect(":memory:")

    conversion_plan = _harvestman.execute(conversion_plan_query).df()

    _harvestman.close()

    return conversion_plan


def round_to_n_sf(value, n: int):

    return float("{:g}".format(float("{:.{p}g}".format(value, p=n))))


def convert_unit_value(value, source_unit, target_unit, precision=None):

    conversion_plan = get_conversion_plan(source_unit, target_unit)

    if (
        len(conversion_plan.unit_type.unique()) == 1
        and conversion_plan.unit_type.unique()[0] == "temperature"
    ):

        for index, row in conversion_plan.iterrows():
            value = (value * row["apply_prefix"] + row["apply_pre_shift"]) * row[
                "apply_conversion"
            ] + row["apply_post_shift"]

        if precision:
            if precision[-2:] == "dp":
                value = round(value, int(precision[:-2]))
            elif precision[-2:] == "sf":
                value = round_to_n_sf(value, int(precision[:-2]))

    else:

        value = (
            value
            * conversion_plan["apply_prefix"].product()
            * conversion_plan["apply_conversion"].product()
        )

    return value


def add_subtract(left, right):

    if (left.sub_units.empty) and (right.sub_units.empty):
        unit = Unit()
    elif left.sub_units.empty:
        raise AttributeError(
            rf"Units are not the same: left: {left.name}, right: {right.name}"
        )
    elif right.sub_units.empty:
        raise AttributeError(
            rf"Units are not the same: left: {left.name}, right: {right.name}"
        )
    elif (
        left.sub_units[["exponent", "symbol"]]
        .sort_values(["exponent", "symbol"])
        .equals(
            right.sub_units[["exponent", "symbol"]].sort_values(["exponent", "symbol"])
        )
    ):
        unit = left
    else:
        raise AttributeError(
            rf"Units are not the same: left: {left.name}, right: {right.name}"
        )

    return unit


def multiply(left, right):

    return Unit(left.name + right.name)


def divide(left, right):

    return Unit(left.name + right.reciprocal().name)


def exponent(unit, exponent):

    new_unit_str = ""

    for index, row in unit.sub_units.iterrows():

        new_unit_str += row["symbol"] + "^" + str(row["exponent"] * exponent)

    return Unit(new_unit_str)
