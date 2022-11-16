from dataclasses import dataclass
from enum import Enum
from typing import List

import pandas as pd

from cin_validator.rule_engine import CINTable, RuleDefinition
from cin_validator.utils import create_issue_locs

# def _as_iterable(value):
#     if isinstance(value, str):
#         return [value]
#     elif isinstance(value, Enum):
#         return [value]
#     return value


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

    def __len__(self):
        # self.type1_issues contains only this object.
        # This dunder method defines what happens when len(self.type1_issues) is run
        return len([self.table])
        # return len(self.row_df)


# class IssueLocatorLists:
#     def __init__(self, table, field, row):
#         self.table = _as_iterable(table)
#         self.field = _as_iterable(field)
#         self.row = _as_iterable(row)

#     def __iter__(self):
#         for table in self.table:
#             for field in self.field:
#                 for row in self.row:
#                     yield IssueLocator(table, field, row)


class RuleContext:
    def __init__(self, definition: RuleDefinition):
        self.__definition = definition
        # TODO create list of rules according to types to prevent checking all attributes each time a rule is run.
        # Possibly classify rule code by adding it to a list of rules with a similar type, when push is done.

        self.__issues = []
        # type1 issues are also initialised here so that errors that don't push to it should still have it as an attribute and not raise an error when checked.
        self.__type1_issues = []
        self.__type2_issues = []
        self.__type3_issues = []

    @property
    def definition(self):
        return self.__definition

    # def push_issue(self, table, field, row):
    #     self.__issues.append(IssueLocatorLists(table, field, row))

    def push_issue(self, table, field, row):
        for i in row:
            self.__issues.append(IssueLocator(table, field, i))

    def push_type_1(self, table, columns, row_df):
        """Many columns, One Table, no merge involved"""
        self.__type1_issues = Type1(table, columns, row_df)

    def push_type_2(self, table, columns, row_df):
        """Multiple columns, multiple tables"""
        table_tuple = Type1(table, columns, row_df)
        self.__type2_issues.append(table_tuple)

    def push_type_3(self, table, columns, row_df):
        """One Table, values are checked per group"""
        table_tuple = Type1(table, columns, row_df)
        self.__type3_issues.append(table_tuple)

    @property
    def issues(self):
        # for issues in self.__issues:
        #     yield from issues
        return self.__issues

    @property
    def type1_issues(self):
        return self.__type1_issues

    @property
    def type2_issues(self):
        return self.__type2_issues

    @property
    def type3_issues(self):
        return self.__type3_issues

    @property
    def type_one_issues(self):
        """expands type1 issue object into a dataframe where each row represents a location in the data
        by a unique table-column-index combination"""
        try:
            # if it is a type1 rule i.e __type1_issues.row_df exists, do this.
            issues = self.__type1_issues
            df_issue_locs = create_issue_locs(issues)
            return df_issue_locs

        except:
            # all non-type1 rules run this.
            return []

    # TODO decide if type_one_issues and type_two_issues should be combined to make them DRY or left apart for readability.
    @property
    def type_two_issues(self):
        """expands type2 issue object into a dataframe where each row represents a location in the data
        by a unique table-column-index combination"""
        try:
            # if it is a type2 rule i.e __type2_issues.row_df exists, do this.
            issues_per_table = self.__type2_issues
            df_issue_locs_lst = []
            for issues in issues_per_table:
                # create a dataframe of issue locations for each table.
                df_issue_loc_table = create_issue_locs(issues)
                # append all table dataframes to a list.
                df_issue_locs_lst.append(df_issue_loc_table)
            # generate a dataframe that contains that data of all tables involved.
            df_issue_locs = pd.concat(df_issue_locs_lst, ignore_index=True)
            return df_issue_locs

        except:
            # all non-type2 rules run this.
            return []

    @property
    def type_three_issues(self):
        """expands type3 issue object into a dataframe where each row represents a location in the data
        by a unique table-column-index combination"""
        try:
            # if it is a type3 rule i.e __type3_issues.row_df exists, do this.
            issues_per_table = self.__type3_issues
            df_issue_locs_lst = []
            for issues in issues_per_table:
                # create a dataframe of issue locations for each table.
                df_issue_loc_table = create_issue_locs(issues)
                # append all table dataframes to a list.
                df_issue_locs_lst.append(df_issue_loc_table)
            # generate a dataframe that contains that data of all tables involved.
            df_issue_locs = pd.concat(df_issue_locs_lst, ignore_index=True)
            return df_issue_locs

        except:
            # all non-type3 rules return this.
            return []
