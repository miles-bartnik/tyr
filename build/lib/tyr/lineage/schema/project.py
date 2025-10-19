from typing import Dict, List, Any
from ..core import TableList
from .core import _Schema, _SchemaSettings


class ProjectSettings(_SchemaSettings):
    def __init__(
        self,
        name,
        substitutions: Dict[Any, Any] = {},
        extensions: List[Dict[str, str]] = [],
        connection: Dict = {},
    ):
        super().__init__(
            name=name,
            substitutions=substitutions,
            extensions=extensions,
            connection=connection,
        )


class Project(_Schema):
    def __init__(self, settings: ProjectSettings):
        super().__init__(tables=TableList([]), settings=settings)
