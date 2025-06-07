from typing import List


class Macro:
    def __init__(self, name: str, function, args: dict):
        self.name = name
        self.args = args
        self.macro = function(**args)
