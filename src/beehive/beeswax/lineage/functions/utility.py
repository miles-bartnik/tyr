from ...lineage import core as lineage
from ...lineage import values as lineage_values
from ...lineage import expressions as lineage_expressions
from typing import List as TypingList, Any
import re
import pandas as pd


class Error(lineage._Function):
    def __init__(self, message: lineage_values.Varchar):
        super().__init__(args=[message], name="ERROR")
