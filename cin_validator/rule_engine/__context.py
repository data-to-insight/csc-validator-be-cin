import pandas as pd
from dataclasses import dataclass
from enum import Enum
from typing import List

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

@dataclass(frozen=True, eq=True)
class Type1:
    table: CINTable
    columns: List[str]
    row_df: pd.DataFrame


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
        

    @property
    def definition(self):
        return self.__definition

    def push_issue(self, table, field, row):
        self.__issues.append(IssueLocatorLists(table, field, row))
    
    def push_type_1(self, table, columns, row_df):
        """Many columns, One Table, no merge involved"""
        self.__type1_issues = Type1(table, columns, row_df)
      
    @property
    def issues(self):
        for issues in self.__issues:
            yield from issues
 

    @property
    def type1_issues(self):
        return self.__type1_issues