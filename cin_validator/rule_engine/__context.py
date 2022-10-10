from collections import defaultdict
from dataclasses import dataclass
from enum import Enum

from cin_validator.rule_engine import RuleDefinition, CINTable


def _as_iterable(value):
    if isinstance(value, str):
        return [value]
    elif isinstance(value, Enum):
        return [value]
    return value


@dataclass(frozen=True, eq=True)
class IssueLocator:
    table: CINTable
    field: str
    row: int


class IssueLocatorLists:
    def __init__(self, table, field, row):
        self.table = _as_iterable(table)
        self.field = _as_iterable(field)
        self.row = _as_iterable(row)

    def __iter__(self):
        for table in self.table:
            for field in self.field:
                for row in self.row:
                    yield IssueLocator(table, field, row)

class RuleContext:
    def __init__(self, definition: RuleDefinition):
        self.__definition = definition
        self.__issues = []
        self.__linked_issues = defaultdict(list)
        

    @property
    def definition(self):
        return self.__definition

    def push_issue(self, table, field, row):
        self.__issues.append(IssueLocatorLists(table, field, row))
    
    def push_linked_issues(self, list_args):
        for tup in list_args:
            table, field, row, id_col = tup
            locators = []
            locators.append(IssueLocatorLists(table, field, row))
            locators_dict = dict(zip(id_col, locators))
            for k, v in locators_dict.items():
                self.__linked_issues[k].append(v)
            # combine the dicts from each loop
    @property
    def issues(self):
        for issues in self.__issues:
            yield from issues
 

    @property
    def linked_issues(self):
        for related_locs in self.__linked_issues.values():
            for loc in related_locs:
                yield from loc