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

"""class LinkedIssueLocatorLists:
    def __init__(self, table, field, row, id_col):
        self.table = _as_iterable(table)
        self.field = _as_iterable(field)
        self.row = _as_iterable(row)
        self.id_col = _as_iterable(id_col)
        # TODO check that id_col and row always have the same length.
        self.issues_per_id = defaultdict(list)

    def __iter__(self):
        for table in self.table:
            for field in self.field:
                # for row in self.row:
                for i in range(len(self.row)):
                    # yield IssueLocator(table, field, row)
                    self.issues_per_id[self.id_col[i]].append(IssueLocator(table, field, self.row[i]))
"""

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
    
    def _accum_issues(self, table, field, row, id_col):
        for i in range(len(row)):
            self.__linked_issues[id_col[i]].append(IssueLocatorLists(table, field, [row[i]]))
            # self.__linked_issues[id_col[i]].append(IssueLocator(table, field, row[i]))

    def push_linked_issues(self, list_args):
        for tup in list_args:
            table, field, row, id_col = tup
            self._accum_issues(table, field, row, id_col)
            
    @property
    def issues(self):
        for issues in self.__issues:
            yield from issues
 

    @property
    def linked_issues(self):
        for linked_issues in self.__linked_issues.values():
            # yield from linked_issues
            yield linked_issues