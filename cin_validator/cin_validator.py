import copy
import xml.etree.ElementTree as ET
from typing import Optional

import pandas as pd

from cin_validator.ingress import XMLtoCSV
from cin_validator.rule_engine import CINTable, RuleContext
from cin_validator.ruleset import create_registry
from cin_validator.utils import process_date_columns

pd.options.mode.chained_assignment = None
# Suppresses false-positive SettingWithCopyError when column types are changes in the include_issue_child function.
# https://stackoverflow.com/questions/20625582/how-to-deal-with-settingwithcopywarning-in-pandas


def enum_keys(dict_input: dict):
    """
    Convert keys of a dictionary to its corresponding CINTable format.
    :param dict dict_input: dictionary of dataframes of CIN data
    :return dict enumed_dict: same data content with keys replaced.
    """
    enumed_dict = {}
    for enum_key in CINTable:
        # if enum_key == CINTable.Header, then enum_key.name == Header
        enumed_dict[enum_key] = dict_input[str(enum_key.name)]
    return enumed_dict


def convert_data(root: ET.Element):
    """
    Takes input data and processes it for validation.

    This function takes input XML data, and uses ElementTree, and the custom class
    XMLtoCSV to process the data into tables for validation.

    :param XML root: root created by parsing the user's xml file.
    :returns: dict of DataFrames - each representing a CIN table.
    :rtype: Dictionary
    """

    # generate tables
    data_files = XMLtoCSV(root)

    # return tables
    cin_tables = {
        "Header": data_files.Header,
        "ChildIdentifiers": data_files.ChildIdentifiers,
        "ChildCharacteristics": data_files.ChildCharacteristics,
        "ChildProtectionPlans": data_files.ChildProtectionPlans,
        "CINdetails": data_files.CINdetails,
        "CINplanDates": data_files.CINplanDates,
        "Reviews": data_files.Reviews,
        "Section47": data_files.Section47,
        "Assessments": data_files.Assessments,
        "Disabilities": data_files.Disabilities,
    }
    return cin_tables


def process_data(cin_tables: dict):
    """
    formats date columns
    :param dict cin_tables: data to be converted
    :return dict cin_tables_dict: original dataframes where date columns have been formatted.
    """

    # format all date columns in tables
    cin_tables_dict = {
        name: process_date_columns(table) for name, table in cin_tables.items()
    }

    return cin_tables_dict


def include_issue_child(issue_df: pd.DataFrame, cin_data: dict):
    """
    :param DataFrame issue_df: complete data about all issue locations.
    :param dict cin_data: dictionary of dataframes generated when cin xml is converted to tabular format.
    """

    try:
        la_level_issues = issue_df[issue_df["tables_affected"].isna()]
    except:
        # if no error locations were found, i.e issue_df doesn't exist, this allows an empty dataframe to be processed.
        return issue_df

    header_issues = issue_df[issue_df["tables_affected"] == "Header"]
    tables_with_childid = [la_level_issues, header_issues]
    for table in issue_df["tables_affected"].dropna().unique():
        if table == "Header":
            # the header table doesn't contain child id. It is like metadata
            continue
        table_df = issue_df[issue_df["tables_affected"] == table]

        # get index values of the rows that fail.
        # some ROW_ID values exist as ints and others as strs. Unify so that .unique() doesn't contain doubles.
        table_rows = table_df["ROW_ID"].astype("int").unique()

        # naming the index of the data allows it to be mapped back to the issue_df
        table_data = cin_data[table]
        table_data.index.name = "ROW_ID"
        table_data.reset_index(inplace=True)
        # select the data for the rows with appear in issue_df and get the child ids
        linker_df = table_data.iloc[table_rows][["LAchildID", "ROW_ID"]]

        # work around: ensure that columns from both sources have the same type to prevent merge error
        table_df["ROW_ID"] = table_df["ROW_ID"].astype("int64")
        linker_df["ROW_ID"] = linker_df["ROW_ID"].astype("int64")

        if not linker_df.empty:
            # if failing locations have been found, remove the dummy LAchildID column to make way for the real one.
            table_df.drop(columns="LAchildID", inplace=True)

        # map the child ids back to issue_df
        table_df = table_df.merge(linker_df, on=["ROW_ID"], how="left")

        # save the result
        tables_with_childid.append(table_df)

    # regenerate issue_df from its updated constituent tables
    issue_df = pd.concat(tables_with_childid)

    return issue_df


def create_user_report(issue_df: pd.DataFrame, cin_data: dict):
    """
    A good report should tell the user what failed, where it failed and why it failed.
    The report generated by this function contains table-column-value combinations to answer the former
    and rule code-description combinations to answer the latter.

    :param pd.DataFrame issue_df: in which child IDs have been added.
    :param dict cin_data: dataframes of user's input data.
    :return user_report: dataframe containing issue locations and specific values that fail in those locations.

    """
    try:
        no_table = issue_df[issue_df["tables_affected"].isna()]
    except:
        # in the case where issue_df is empty, return an empty user report.
        return pd.DataFrame()

    reports = []
    for table in issue_df["tables_affected"].dropna().unique():
        table_issues = issue_df[issue_df["tables_affected"] == table]

        table_reports = []
        for column in table_issues["columns_affected"].unique():
            only_column = table_issues[table_issues["columns_affected"] == column]
            column_rows = only_column["ROW_ID"].unique().astype("int")

            column_data = cin_data[table][column]
            # fancy indexing. get all the values for a sequence of row positions in a column.
            column_values = column_data[column_rows]
            column_values.rename("value_flagged", inplace=True)
            column_values.index.name = "ROW_ID"
            values_df = column_values.reset_index()
            values_df = values_df.assign(ROW_ID=values_df["ROW_ID"].astype("object"))

            # work around: ensure that columns from both sources (user data and rule output) have the same type to prevent merge error
            only_column["ROW_ID"] = only_column["ROW_ID"].astype("int64")
            values_df["ROW_ID"] = values_df["ROW_ID"].astype("int64")
            report_df = only_column.merge(values_df, on="ROW_ID")

            table_reports.append(report_df)

        reports.extend(table_reports)
    # add in the la-level locations
    reports.append(no_table)

    # ensure that all required column names will be present in the result.
    full_report_cols_df = pd.DataFrame(
        columns=[
            "ERROR_ID",
            "LAchildID",
            "rule_code",
            "tables_affected",
            "columns_affected",
            "ROW_ID",
            "value_flagged",
            "rule_description",
        ]
    )
    reports.append(full_report_cols_df)

    full_report = pd.concat(reports, ignore_index=True)

    # columns of interest are filtered and arranged in the desired order. values unified under str datatype.
    user_report = full_report[
        [
            "ERROR_ID",
            "LAchildID",
            "rule_code",
            "tables_affected",
            "columns_affected",
            "ROW_ID",
            "value_flagged",
            "rule_description",
        ]
    ]

    def datetime_to_str(element):
        if isinstance(element, pd.Timestamp):
            # convert datetime elements to str date values
            return str(element.strftime("%Y-%m-%d"))
        elif isinstance(element, tuple):
            # loop through tuples and convert each element accordingly. mostly in ERROR_ID column.
            return tuple(map(datetime_to_str, element))
        else:
            # ensure all other elements are strings too.
            return str(element)

    user_report = user_report.applymap(datetime_to_str)

    # Related issue locations should be displayed next to each other.
    user_report.sort_values(
        [
            "LAchildID",
            "ERROR_ID",
            "tables_affected",
            "columns_affected",
        ],
        inplace=True,
        ignore_index=True,
    )

    user_report.drop_duplicates(
        ["LAchildID", "rule_code", "columns_affected", "ROW_ID"], inplace=True
    )

    return user_report


class CinValidator:
    """
    A class to contain the process of CIN validation. Generates error reports as dataframes.

    :param any data_files: Data files for validation, either a DataContainerWrapper object, or a
        dictionary of DataFrames.
    :param dir ruleset: The directory containing the validation rules to be run according to the year in which they were published.
    """

    def __init__(
        self,
        ruleset,
        data_files=None,
        selected_rules: Optional[list[str]] = None,
    ) -> None:
        """
        Initialises CinValidator class.

        Creates DataFrame containing error report, and allows selection of individual instances of error using ERROR_ID

        :param list ruleset: The list of rules used in an individual validation session.
            Refers to rules in particular subdirectories of the rules directory.
        :param any data_files: The data extracted from input XML (or CSV) for validation.
        :param str issue_id: Can be used to choose a particular instance of an error using ERROR_ID.
        :param list selected_rules: array of rule codes (as strings) selected by the user. Determines what rules should be run.
        :returns: DataFrame of error report which could be a filtered version if issue_id is input.
        :rtype: DataFrame
        """

        self.data_files = data_files
        self.ruleset = ruleset

        # save independent version of data to be used in report.
        raw_data = copy.deepcopy(self.data_files)

        # run
        self.create_issue_report_df(selected_rules)

        # add child_id to issue location report.
        self.full_issue_df: pd.DataFrame = include_issue_child(
            self.full_issue_df, raw_data
        )
        self.user_report = create_user_report(self.full_issue_df, raw_data)

        # regularise full_issue_df
        self.full_issue_df.rename(columns={"ROW_ID": "row_id"}, inplace=True)
        self.full_issue_df.rename(columns={"LAchildID": "child_id"}, inplace=True)
        self.full_issue_df.drop(columns=["ERROR_ID"], inplace=True, errors="ignore")
        self.full_issue_df.drop_duplicates(
            ["child_id", "rule_code", "columns_affected", "row_id"], inplace=True
        )

    def get_rules_to_run(self, registry, selected_rules: Optional[list[str]] = None):
        """
        Filters rules to be run based on user's selection in the frontend.
        :param Registry-class registry: record of all existing rules in rule pack
        :param list selected_rules: array of rule codes as strings
        """
        if selected_rules:
            rules_to_run = [
                rule for rule in registry if str(rule.code) in selected_rules
            ]
            return rules_to_run
        else:
            return registry

    def process_issues(self, rule, ctx):
        """
        process result of running a rule on the user's data.

        :param RuleDefinition-class rule: the rule that was run on the data
        :param RuleContext-object ctx: "manages state" per rule. contains updated issue_dfs if any were added when rule was run on the data.
        :returns : None

        """
        # TODO is it wiser to split the rules according to types instead of checking the type each time a rule is run?.
        issue_dfs_per_rule = pd.Series(
            [
                ctx.type_zero_issues,
                ctx.type_one_issues,
                ctx.type_two_issues,
                ctx.type_three_issues,
                ctx.la_level_issues,
            ]
        )
        # error_df_lengths is a list of lengths of all elements in issue_dfs_per_rule respectively.
        error_df_lengths = pd.Series([len(x) for x in issue_dfs_per_rule])
        if error_df_lengths.max() == 0:
            # if the rule didn't push to any of the issue accumulators, then it didn't find any issues in the file.
            self.rules_passed.append(rule.code)
        elif error_df_lengths.idxmax() == 4:
            # If the maximum value is in position 4, this is a return level validation rule.
            # It has no locations attached so it is only displayed in the rule descriptions.
            self.la_rules_broken.append(issue_dfs_per_rule[4])
        else:
            # get the rule type based on which attribute had elements pushed to it (i.e non-zero length)
            # its corresponding error_df can be found by issue_dfs_per_rule[ind]
            ind = error_df_lengths.idxmax()

            issue_dict = {
                "code": rule.code,
                "number": error_df_lengths[ind],
                "type": ind,
            }
            issue_dict_df = pd.DataFrame([issue_dict])
            self.issue_instances = pd.concat(
                [self.issue_instances, issue_dict_df], ignore_index=True
            )

            # add the rule's code and description to it's error_df
            issue_dfs_per_rule[ind]["rule_code"] = rule.code
            issue_dfs_per_rule[ind]["rule_description"] = rule.message

            # temporary: add rule type to track if all types are in df.
            issue_dfs_per_rule[ind]["rule_type"] = ind

            # combine this rule's error_df with the cummulative error_df
            self.full_issue_df = pd.concat(
                [self.full_issue_df, issue_dfs_per_rule[ind]],
                ignore_index=True,
            )

            # Elements of the rule_descriptors df to explain error codes
            self.rules_broken.append(rule.code)
            self.rule_messages.append(f"{str(rule.code)} - {rule.message}")

    def create_issue_report_df(self, selected_rules: Optional[list[str]] = None):
        """
        Creates report of errors found when validating CIN data input to
        the tool.

        This function takes the errors/rule violations reported by individual validation rule functions,
        including table, field, and index locations of errors. It is important that it uses deepcopy
        on the data per rule as some rules alter original data when only a standard .copy() function
        is used. It runs through every rule in the registry and:

        >Creates lists of rules passed, broken, and relevant messages.
        >Returns a dataframe of issue instances for broken validation rules.
        >Returns a dictionary of all rules codes and relevant messages.

        :param DataFrame issue_instances: issues found in validation.
        :param DataFrame all_rules_issue_locs: issue locations
        :param list rules_broken: An empty list which is populated with the codes of the rules that trigger issues in the data during validation.
        :param list la_rules_broken: An empty list which is populated with the list of LA rules that fail validation.
        :param list rules_passed: An empty list of rules passed, populated with rules with no validation errors.
        :returns: DataFrame of instances and locations of validation rule violations from data input via FE or CLI.
        :rtype: DataFrame
        :raises: Errors with rules that raise errors when validating data.
        """

        enum_data_files = enum_keys(self.data_files)
        self.issue_instances = pd.DataFrame()
        self.full_issue_df = pd.DataFrame(
            columns=[
                "tables_affected",
                "columns_affected",
                "ROW_ID",
                "ERROR_ID",
                "rule_code",
                "rule_description",
                "rule_type",
                "la_level",
                "LAchildID",
            ]
        )
        self.rules_passed: list[str] = []

        self.rules_broken: list[str] = []
        self.rule_messages: list[str] = []
        self.la_rules_broken: list[str] = []

        registry = create_registry(self.ruleset)

        rules_to_run = self.get_rules_to_run(registry, selected_rules)
        for rule in rules_to_run:
            data_files = copy.deepcopy(enum_data_files)
            ctx = RuleContext(rule)
            try:
                rule.func(data_files, ctx)
            except Exception as e:
                print(f"Error with rule {rule.code}: {type(e).__name__}, {e}")
            self.process_issues(rule, ctx)

        # df of all broken rule codes and related error messages.
        child_level_rules = pd.DataFrame(
            {"Rule code": self.rules_broken, "Rule Message": self.rule_messages}
        )
        # self.la_rules_broken is a list of issue_dfs, one per la-level rule that failed.
        try:
            self.la_rule_issues = pd.concat(self.la_rules_broken)
        except:
            # if la_rules_broken is still an empty list
            self.la_rule_issues = pd.DataFrame()
