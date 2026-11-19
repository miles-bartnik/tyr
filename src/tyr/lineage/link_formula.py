"""
Linked-column formula form.

``link_mapping`` accepts a small expression DSL alongside the dict form::

    [link_column * 2 + 1] | "0"      numeric: arithmetic in DOUBLE
    {upper(link_column) || '_x'}     string: key cast to VARCHAR first

The expression is wrapped in EXACTLY ONE marker:

- ``[...]`` numeric mode: the ``link_column`` identifier resolves to the link
  column's transformed staging expression (cast to DOUBLE so arithmetic never
  integer-truncates); the result is cast to the target column's data_type.
- ``{...}`` string mode: the ``link_column`` identifier resolves to the link
  column's transformed staging expression cast to VARCHAR; string operators
  apply; the result is cast to the target column's data_type.

``link_column`` is the only legal identifier. Constants are numbers (``3.0``)
and single-quoted strings (``'ab'``). Operators: ``+ - * / %``, ``**``
(exponent; binds tighter than ``* / %``, right-associative), ``||`` (string
mode only) and parentheses. Functions (arity-checked):

- numeric: ``floor ceil round abs min max``
- string: ``upper lower trim left(x, n) right(x, n) length replace(x, from, to)``

The ``| "fallback"`` segment is optional and matches the dict form's
semantics: a NULL link key substitutes the fallback, otherwise the target is
left unchanged. Division by a non-constant zero raises a tyr Error() at
runtime; cast failures raise at runtime.

Formula arithmetic is unit-agnostic: the wrapped key expression's unit is
neutralised so unit-bearing link columns can still be used, and the result is
always cast to the target column's data_type.
"""

import re

from .units.core import Unit

from . import core as lineage
from . import values as lineage_values
from . import expressions as lineage_expressions
from . import functions as lineage_functions

IDENTIFIER = "link_column"

NUMERIC_MODE = "numeric"
STRING_MODE = "string"

# name -> (min_arity, max_arity); None = unbounded
_NUMERIC_FUNCTIONS = {
    "floor": (1, 1),
    "ceil": (1, 1),
    "round": (1, 2),
    "abs": (1, 1),
    "min": (2, None),
    "max": (2, None),
}

_STRING_FUNCTIONS = {
    "upper": (1, 1),
    "lower": (1, 1),
    "trim": (1, 1),
    "left": (2, 2),
    "right": (2, 2),
    "length": (1, 1),
    "replace": (3, 3),
}

_MARKER_PAIRS = {"[": "]", "{": "}"}

_TOKEN_RE = re.compile(
    r"""
    (?P<number>\d+(?:\.\d+)?)
    | (?P<string>'(?:[^']|'')*')
    | (?P<identifier>[A-Za-z_][A-Za-z0-9_]*)
    | (?P<operator>\|\||\*\*|[+\-*/%(),])
    | (?P<whitespace>\s+)
    | (?P<bad>.)
    """,
    re.VERBOSE,
)


def is_link_formula(link_mapping: str) -> bool:
    """
    Shape detection for the formula form. ``[...]`` is always a formula; ``{...}``
    is a formula unless it looks like a JSON dict (a ``"`` right after the brace --
    JSON object keys are always quoted), which keeps dict-vs-formula detection
    unambiguous.
    """
    stripped = str(link_mapping).strip()

    if stripped.startswith("["):
        return True

    return stripped.startswith("{") and not re.match(r'^\{\s*"', stripped)


def split_link_formula(link_mapping: str):
    """
    Split ``'<expr>' | "fallback"`` at the last top-level `` | `` that sits
    outside single-quoted string constants (so ``{'a | b' || link_column}``
    is not misparsed). Returns ``(expr_text, fallback)``; ``fallback`` is
    ``None`` when the segment is absent, with surrounding quotes stripped when
    present.
    """
    text = str(link_mapping)
    fallback = None
    split_at = None
    in_string = False
    index = 0

    while index < len(text):
        char = text[index]

        if in_string:
            if char == "'":
                if index + 1 < len(text) and text[index + 1] == "'":
                    index += 1
                else:
                    in_string = False
        elif char == "'":
            in_string = True
        elif text[index : index + 3] == " | ":
            split_at = index

        index += 1

    if split_at is not None:
        expr_text = text[:split_at].strip()
        fallback = text[split_at + 3 :].strip()

        if len(fallback) >= 2 and fallback[0] in "\"'" and fallback[-1] == fallback[0]:
            fallback = fallback[1:-1]

        if not fallback:
            raise ValueError(rf"link formula fallback segment is empty: {link_mapping}")
    else:
        expr_text = text.strip()

    return expr_text, fallback


def parse_link_formula(link_mapping: str, link_key_expression=None):
    """
    Parse and validate the formula form of a link_mapping. Returns
    ``(expression, mode, fallback)`` where ``expression`` is the compiled tyr
    lineage expression -- NOT yet cast to the target column's data_type --
    and ``mode`` is ``"numeric"`` or ``"string"``.

    ``link_key_expression`` is the link column's transformed staging
    expression; every ``link_column`` reference resolves to it (wrapped per
    mode). When omitted (build-time validation), a typed placeholder stands
    in for it.
    """
    expr_text, fallback = split_link_formula(link_mapping)

    opener = expr_text[0] if expr_text else ""
    closer = expr_text[-1] if expr_text else ""

    if opener not in _MARKER_PAIRS:
        raise ValueError(rf"link formula must start with '[' or '{{': {link_mapping}")

    if closer != _MARKER_PAIRS[opener]:
        raise ValueError(
            rf"link formula must be wrapped in exactly one '{opener}{_MARKER_PAIRS[opener]}' marker pair: {link_mapping}"
        )

    mode = NUMERIC_MODE if opener == "[" else STRING_MODE
    body = expr_text[1:-1].strip()

    if not body:
        raise ValueError(rf"link formula is empty: {link_mapping}")

    expression = _Parser(body, mode, link_key_expression).parse()

    return expression, mode, fallback


class _Parser:
    """
    Regex tokenizer + recursive-descent parser. Builds tyr lineage expressions
    directly: no intermediate AST. Mode-consistency rules (arithmetic only in
    numeric mode, ``||`` and string constants only in string mode, functions
    restricted to their mode) are enforced as tokens are consumed, so a
    formula that violates its marker raises a ValueError at parse time.
    """

    def __init__(self, source, mode, link_key_expression):
        self.source = source
        self.mode = mode
        self.link_key_expression = link_key_expression
        self._wrapped_key = None
        self.tokens = self._tokenize(source)
        self.pos = 0

    # tokens

    def _tokenize(self, source):
        tokens = []

        for match in _TOKEN_RE.finditer(source):
            kind = match.lastgroup
            text = match.group()

            if kind == "whitespace":
                continue

            if kind == "bad":
                raise ValueError(
                    rf"link formula: unexpected character {text!r} in [{self.source}]"
                )

            tokens.append((kind, text))

        tokens.append(("end", ""))

        return tokens

    def _peek(self):
        return self.tokens[self.pos]

    def _next(self):
        token = self.tokens[self.pos]
        self.pos += 1
        return token

    def _accept(self, kind, value=None):
        token_kind, token_value = self._peek()

        if token_kind == kind and (value is None or token_value == value):
            self.pos += 1
            return True

        return False

    def _expect(self, kind, value=None):
        token_kind, token_value = self._peek()

        if token_kind != kind or (value is not None and token_value != value):
            expected = value if value is not None else kind
            raise ValueError(
                rf"link formula: expected {expected!r} but found {token_value!r} in [{self.source}]"
            )

        return self._next()

    # grammar
    #   concat     := additive (|| additive)*                string mode only
    #   additive   := multiplicative ((+|-) multiplicative)*
    #   multiplicative := power ((*|/|%) power)*
    #   power      := unary (** power)?                       right-associative
    #   unary      := (- | +)* primary
    #   primary    := number | string | identifier | call | ( concat )

    def parse(self):
        expression = self._parse_concat()
        self._expect("end")
        return expression

    def _parse_concat(self):
        left = self._parse_additive()

        while self._accept("operator", "||"):
            self._require_mode(STRING_MODE, "'||' concatenation")
            left = lineage_expressions.Concat(left, self._parse_additive())

        return left

    def _parse_additive(self):
        left = self._parse_multiplicative()

        while self._peek() in [("operator", "+"), ("operator", "-")]:
            operator = self._next()[1]
            self._require_numeric_operator(operator)
            right = self._parse_multiplicative()

            if operator == "+":
                left = lineage_functions.math.Add(left, right)
            else:
                left = lineage_functions.math.Subtract(left, right)

        return left

    def _parse_multiplicative(self):
        left = self._parse_power()

        while self._peek() in [("operator", "*"), ("operator", "/"), ("operator", "%")]:
            operator = self._next()[1]
            self._require_numeric_operator(operator)
            right = self._parse_power()

            if operator == "*":
                left = lineage_functions.math.Multiply(left, right)
            else:
                left = self._guarded_division(left, right, operator)

        return left

    def _parse_power(self):
        base = self._parse_unary()

        if self._accept("operator", "**"):
            self._require_mode(NUMERIC_MODE, "'**' exponentiation")
            # recurse on the exponent side: right-associative
            base = lineage_functions.math.Exponent(base, self._parse_power())

        return base

    def _parse_unary(self):
        if self._accept("operator", "-"):
            self._require_numeric_operator("-")
            return lineage_functions.math.Subtract(
                lineage_values.FloatingPoint(0.0), self._parse_unary()
            )

        if self._accept("operator", "+"):
            self._require_numeric_operator("+")
            return self._parse_unary()

        return self._parse_primary()

    def _parse_primary(self):
        kind, value = self._peek()

        if kind == "number":
            self._next()
            return self._number(value)

        if kind == "string":
            self._next()
            self._require_mode(STRING_MODE, "string constants")
            # values_varchar renders without escaping; double single quotes to bind
            return lineage_values.Varchar(value[1:-1].replace("''", "'").replace("'", "''"))

        if kind == "identifier":
            self._next()

            if value == IDENTIFIER:
                if self._peek() == ("operator", "("):
                    raise ValueError(
                        rf"link formula: '{IDENTIFIER}' is a column reference, not a function: [{self.source}]"
                    )

                return self._key()

            if self._peek() == ("operator", "("):
                return self._parse_call(value)

            raise ValueError(
                rf"link formula: unknown identifier '{value}' in [{self.source}]. "
                rf"'{IDENTIFIER}' is the only legal identifier"
            )

        if self._accept("operator", "("):
            expression = self._parse_concat()
            self._expect("operator", ")")
            return expression

        raise ValueError(
            rf"link formula: unexpected token {value!r} in [{self.source}]"
        )

    # identifiers / functions

    def _key(self):
        if self._wrapped_key is not None:
            return self._wrapped_key

        if self.link_key_expression is not None:
            key = self.link_key_expression
        elif self.mode == STRING_MODE:
            key = lineage_values.Varchar("")
        else:
            key = lineage_values.Integer(0)

        if self.mode == STRING_MODE:
            if key.data_type == lineage_values.Datatype("VARCHAR"):
                wrapped = key
            else:
                wrapped = lineage_functions.data_type.Cast(
                    key, lineage_values.Datatype("VARCHAR")
                )
        else:
            # arithmetic in DOUBLE so e.g. / never integer-truncates
            wrapped = lineage_functions.data_type.Cast(
                key, lineage_values.Datatype("DOUBLE")
            )

        # formulas are unit-agnostic; mixing a unit-bearing column with
        # unitless constants would trip the unit arithmetic in the math nodes
        wrapped.unit = Unit()

        self._wrapped_key = wrapped

        return wrapped

    def _parse_call(self, name):
        functions = _STRING_FUNCTIONS if self.mode == STRING_MODE else _NUMERIC_FUNCTIONS

        if name not in functions:
            other = (
                _NUMERIC_FUNCTIONS
                if self.mode == STRING_MODE
                else _STRING_FUNCTIONS
            )

            if name in other:
                raise ValueError(
                    rf"link formula: function '{name}' is not allowed in a {self.mode} formula: [{self.source}]"
                )

            raise ValueError(
                rf"link formula: unknown function '{name}' in [{self.source}]"
            )

        min_arity, max_arity = functions[name]
        args = []

        self._expect("operator", "(")

        if not self._accept("operator", ")"):
            while True:
                args.append(self._parse_concat())

                if self._accept("operator", ","):
                    continue

                self._expect("operator", ")")
                break

        if len(args) < min_arity or (max_arity is not None and len(args) > max_arity):
            if max_arity is None:
                expected = rf"at least {min_arity}"
            elif min_arity == max_arity:
                expected = str(min_arity)
            else:
                expected = rf"{min_arity} or {max_arity}"

            raise ValueError(
                rf"link formula: function '{name}' takes {expected} argument(s), got {len(args)}: [{self.source}]"
            )

        return self._build_function(name, args)

    def _build_function(self, name, args):
        math = lineage_functions.math
        string = lineage_functions.string

        if name == "floor":
            return math.Floor(args[0])

        if name == "ceil":
            return math.Ceiling(args[0])

        if name == "round":
            precision = args[1] if len(args) == 2 else lineage_values.Integer(0)
            return math.Round(args[0], precision)

        if name == "abs":
            return math.AbsoluteValue(args[0])

        if name == "min":
            return math.Least(args)

        if name == "max":
            return math.Greatest(args)

        if name == "upper":
            return string.Upper(args[0])

        if name == "lower":
            return string.Lower(args[0])

        if name == "trim":
            return string.Trim(args[0])

        if name == "left":
            return string.LeftExtract(args[0], args[1])

        if name == "right":
            return string.RightExtract(args[0], args[1])

        if name == "length":
            return string.Length(args[0])

        if name == "replace":
            return string.Replace(args[0], args[1], args[2])

        raise ValueError(rf"link formula: unimplemented function '{name}'")

    # mode / safety

    def _require_mode(self, mode, construct):
        if self.mode != mode:
            marker = "[" if mode == NUMERIC_MODE else "{"
            raise ValueError(
                rf"link formula: {construct} is only allowed in a {mode} formula {marker}...{self._close(marker)}: [{self.source}]"
            )

    @staticmethod
    def _close(marker):
        return "]" if marker == "[" else "}"

    def _require_numeric_operator(self, operator):
        if self.mode != NUMERIC_MODE:
            raise ValueError(
                rf"link formula: arithmetic operator '{operator}' is not allowed in a string formula {{...}}: [{self.source}]"
            )

    def _guarded_division(self, left, right, operator):
        math = lineage_functions.math

        if isinstance(right, (lineage_values.Integer, lineage_values.FloatingPoint)):
            if float(right.value) == 0.0:
                raise ValueError(
                    rf"link formula: division by a zero constant in [{self.source}]"
                )

            # nonzero constant divisor: no runtime zero possible
            if operator == "/":
                return math.Divide(left, right)
            return math.Mod(left, right)

        division = (
            math.Divide(left, right) if operator == "/" else math.Mod(left, right)
        )

        # runtime guard: a tyr Error() on a zero divisor, so a bad row fails
        # loudly instead of silently producing NULL or an engine error
        guard = lineage.CaseWhen(
            conditions=[
                lineage.Condition(
                    checks=[
                        lineage_expressions.Equal(
                            right, lineage_values.FloatingPoint(0.0)
                        )
                    ]
                )
            ],
            values=[
                lineage_functions.utility.Error(
                    lineage_values.Varchar(
                        rf"link formula: division by zero in [{self.source}]"
                    )
                )
            ],
            else_value=division,
        )
        # the Error branch is untyped; give the CASE the division's type so the
        # guard can feed further arithmetic
        guard.data_type = division.data_type
        guard.var_type = "numeric"

        return guard

    @staticmethod
    def _number(text):
        if "." in text:
            return lineage_values.FloatingPoint(float(text))

        return lineage_values.Integer(int(text))
