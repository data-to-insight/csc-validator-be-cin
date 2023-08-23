from dataclasses import dataclass
from typing import List

import pandas as pd

from cin_validator.rule_engine import CINTable, RuleDefinition
from cin_validator.utils import create_issue_locs


@dataclass(frozen=True, eq=True)
class IssueLocator:
    """
    Dataclass used to specify definite locations of issues that should be highlighted in the user's data.

    :param CINTable-object table: Contains the name of the module/table in erorr for
        a validation rule.
    :param str field: The name of the field/column of an error in validation.
    :param int row: The index/row number of an error.
    :returns: IssueLocator object containing table, field, and row of validation errors.
    :rtype: IssueLocator object.
    """

    table: CINTable
    field: str
    row: int


@dataclass(frozen=True, eq=True)
class Type1:
    """Dataclass to define issue locations when more than one column is involved."""

    table: CINTable
    columns: List[str]
    row_df: pd.DataFrame


class RuleContext:
    """
    The RuleContext class includes methods that define how error locations
    should be stored per validation rule.

    >Type 0 rules contain 1 table and 1 column.
    >Type 1 rules contain multiple columns, but one table and no merges.
    >Type 2 rules contain multiple tables and columns.
    >Type 3 rules contain  one table but use merges to check values by group,
        e.g. a CINplan group.
    >LA level rules contain checks for a whole local authority.
    """

    def __init__(self, definition: RuleDefinition):
        """
        Initialises RuleContext class.

        :param RuleDefinition-object definition: Member of the rule definition dataclass,
            contains information about each validation rule.
        :param list issues: Empty list to be populated with type 0 and 1 issues.
        :param list type2_issues: Empty list to be populated with type 2 issues.
        :param list type3_issues: Empty list to be populated with type 3 issues.
        """

        self.__definition = definition

        self.__issues: list = []
        self.__type1_issues: list = []
        self.__type2_issues: list = []
        self.__type3_issues: list = []
        self.__la_issues: list = []

    @property
    def definition(self):
        """
        Used to call information about validation rules.

        :returns: Object containing information about each validation rule.
        :rtype: RuleDefinition object.
        """

        return self.__definition

    # TODO create list of rules according to types to prevent checking all attributes each time a rule is run.
    # Possibly classify rule code by adding it to a list of rules with a similar type, when push is done.

    # METHODS THAT DEFINE HOW ERROR LOCATIONS SHOULD BE STORED PER RULE STRUCTURE
    def push_issue(self, table, field, row):
        """
        For rules that check only a single column.

        :param CINTable-object table: the table a validation error ocurred in.
        :param CINTable-object column: the column a validation error ocurred in.
        :param DataFrame row_df: errors for a validation rule by table.
        :returns: information to locate validation errors in original data.
        :rtype: list of IssueLocator objects.
        """

        for i in row:
            self.__issues.append(IssueLocator(table, field, i))

    def push_type_1(self, table, columns, row_df):
        """
        For rules that check multiple columns in a single table, no merge involved.

        :param CINTable-object table: the table a validation error ocurred in.
        :param CINTable-object column: the column a validation error ocurred in.
        :param DataFrame row_df: the errors for a validation rule by table.
        :returns: information to locate validation errors in original data.
        :rtype: dataclass object
        """

        self.__type1_issues = Type1(table, columns, row_df)

    def push_type_2(self, table, columns, row_df):
        """
        For rules that check multiple columns across multiple tables.

        :param CINTable-object table: the table a validation error ocurred in.
        :param CINTable-object column:the column a validation error ocurred in.
        :param DataFrame row_df: the errors for a validation rule by table.
        :returns: information to locate validation errors in original data.
        :rtype: list of dataclass objects
        """

        table_tuple = Type1(table, columns, row_df)
        self.__type2_issues.append(table_tuple)

    def push_type_3(self, table, columns, row_df):
        """
        For rules that check values in a group with respect to each other.

        :param CINTable-object table: the table a validation error ocurred in.
        :param CINTable-object column: the column a validation error ocurred in.
        :param DataFrame row_df: the errors for a validation rule by table.
        :returns: information to locate validation errors in original data.
        :rtype: list of dataclass objects
        """

        table_tuple = Type1(table, columns, row_df)
        self.__type3_issues.append(table_tuple)

    def push_la_level(self, rule_code, rule_description):
        """
        For rules that check relationships across the whole local authority

        :param CINTable-object table: the table a validation error ocurred in.
        :param CINTable-object column: the column a validation error ocurred in.
        :param DataFrame row_df: the errors for a validation rule by table.
        :returns: information to locate validation errors in original data.
        :rtype: list of tuples
        """

        self.__la_issues = (rule_code, rule_description)

    # PROPERTIES FOR TEST_VALIDATE FUNCTIONS
    @property
    def issues(self):
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
    def la_issues(self):
        return self.__la_issues

    # PROPERTIES FOR CREATING THE ERROR REPORT
    @property
    def type_zero_issues(self):
        """
        Expands issues object into a dataframe where each row represents a location in the data
        by a unique table-column-index combination.

        :returns: DataFrame that contains failing locations of rules that involve only 1 column
        :rtype: DatFrame
        """

        if len(self.__issues) != 0:
            locator_dicts = []
            for locator in self.__issues:
                # convert every IssueLocator object to a dictionary
                locator_as_dict = {
                    "tables_affected": str(locator.table)[9:],
                    "columns_affected": str(locator.field),
                    "ROW_ID": str(locator.row),
                }
                locator_dicts.append(locator_as_dict)
            # create a df that contains data from all issue_locators generated by the rule.
            df_issue_locs = pd.DataFrame(locator_dicts)
            return df_issue_locs

        else:
            # for non-type0 rules, do this
            return []

    @property
    def type_one_issues(self):
        """
        Expands type1 issue object into a dataframe where each row represents a location in the data
        by a unique table-column-index combination.

        :returns: DataFrame that contains failing locations of rules that involve only 1 table
            and multiple columns.
        :rtype: DatFrame
        """
        try:
            # if it is a type1 rule i.e __type1_issues.row_df exists, do this.
            issues = self.__type1_issues
            df_issue_locs = create_issue_locs(issues)
            return df_issue_locs

        except:
            # all non-type1 rules run this.
            return []

    # type_one_issues and type_two_issues, though similar, should be left apart for readability.
    @property
    def type_two_issues(self):
        """
        Expands type2 issue object into a dataframe where each row represents a location in the data
        by a unique table-column-index combination

        :returns: DataFrame that contains failing locations of rules that involve multiple tables.
        :rtype: DataFrame
        """

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
        """
        Expands type3 issue object into a dataframe where each row represents a location in the data
        by a unique table-column-index combination.

        :returns: DataFrame that contains failing locations of rules that involve errors within groups.
        :rtype: DataFrame
        """

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

    @property
    def la_level_issues(self):
        """
        Creates DataFrame of return level validation errors.

        :returns: DataFrame containing rule code/description of la-level rules that the data failed.
        :rtype: DataFrame
        """
        try:
            code, desc = self.__la_issues
            la_df = pd.DataFrame(
                [{"rule_code": code, "rule_description": desc, "la_level": True}]
            )
            return la_df
        except:
            return []
