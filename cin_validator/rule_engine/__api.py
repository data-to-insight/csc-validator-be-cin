from dataclasses import dataclass
from enum import Enum
from typing import Callable, Iterable


class RuleType(Enum):
    ERROR = "Error"


class Module(Enum):
    CHILD_IDENTIFIERS = "Child Identifiers"


@dataclass(frozen=True, eq=True)
class RuleDefinition:
    code: int
    func: Callable
    rule_type: RuleType = RuleType.ERROR
    module: Module = None
    affected_fields: Iterable[str] = None
    description: str = None
