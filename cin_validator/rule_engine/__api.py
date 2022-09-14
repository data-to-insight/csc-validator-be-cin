import importlib
from dataclasses import dataclass
from enum import Enum
from typing import Callable, Iterable


class CINTable(Enum):
    Header = Enum("Header", ["Collection"])
    ChildIdentifiers = Enum(
        "ChildIdentifiers", ["LAchildID", "UPN", "PersonBirthDate", "GenderCurrent"]
    )
    ChildCharacteristics = Enum("ChildCharacteristics", [])
    Disabilities = Enum("Disabilities", [])
    CINdetails = Enum("CINdetails", [])
    Assessments = Enum("Assessments", [])
    CINplanDates = Enum("CINplanDates", [])
    Section47 = Enum("Section47", [])
    ChildProtectionPlans = Enum("ChildProtectionPlans", [])
    Reviews = Enum("Reviews", [])

    def __getattr__(self, item):
        if not item.startswith("_"):
            try:
                return self.value[item].name
            except KeyError as kerr:
                raise AttributeError(f"Table {self.name} has no field {item}") from kerr
        else:
            return super().__getattr__(item)


class RuleType(Enum):
    ERROR = "Error"


@dataclass(frozen=True, eq=True)
class RuleDefinition:
    code: int
    func: Callable
    rule_type: RuleType = RuleType.ERROR
    module: CINTable = None
    affected_fields: Iterable[str] = None
    message: str = None

    @property
    def code_module(self):
        return importlib.import_module(self.func.__module__)
