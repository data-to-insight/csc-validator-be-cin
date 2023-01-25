import copy
import importlib

import pandas as pd

from cin_validator.ingress import XMLtoCSV
from cin_validator.rule_engine import CINTable, RuleContext, registry
from cin_validator.utils import process_date_columns


def enum_keys(dict_input):
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


def process_data(root):
    """
    Takes input data and processes it for validation.

    This function takes input XML data, and uses ElementTree, and the custom class
    XMLtoCSV to process the data into tables for validation. Returning as a dict
    or DataContainerWrapper object. Returns as dict for front-end purposes and object
    for CLI interface.

    :param XML filename: XML files passed from either the front end or the CLI.
    :returns: Data files as object or dict of DataFrames for validation.
    :rtype: Dictionary or DataContainerWrapper object.
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

    # format all date columns in tables
    cin_tables_dict = {
        name: process_date_columns(table) for name, table in cin_tables.items()
    }

    return cin_tables_dict


def include_issue_child(issue_df, cin_data):
    """
    :param DataFrame issue_df: complete data about all issue locations.
    :param dict cin_data: dictionary of dataframes generated when cin xml is converted to tabular format.
    """

    la_level_issues = issue_df[issue_df["tables_affected"].isna()]
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
        # map the child ids back to issue_df
        table_df = table_df.merge(linker_df, on=["ROW_ID"], how="left")

        # save the result
        tables_with_childid.append(table_df)

    # regenerate issue_df from its updated constituent tables
    issue_df = pd.concat(tables_with_childid)

    return issue_df


class CinValidationSession:
    """
    A class to contain the process of CIN validation. Generates error reports as dataframes.

    :param any data_files: Data files for validation, either a DataContainerWrapper object, or a
        dictionary of DataFrames.
    :param dir ruleset: The directory containing the validation rules to be run according to the year in which they were published.
    :param str issue_id: ID of individual errors to be selected for viewing using
        select_by_id method (Error IDs, not rule codes).
    """

    def __init__(
        self,
        data_files=None,
        ruleset="rules.cin2022_23",
        issue_id=None,
        selected_rules=None,
    ) -> None:
        """
        Initialises CinValidationSession class.

        Creates DataFrame containing error report, and allows selection of individual instances of error using ERROR_ID

        :param any data_files: The data extracted from input XML (or CSV) for validation.
        :param list ruleset: The list of rules used in an individual validation session.
            Refers to rules in particular subdirectories of the rules directory.
        :param str issue_id: Can be used to choose a particular instance of an error using ERROR_ID.
        :param list selected_rules: array of rule codes (as strings) selected by the user. Determines what rules should be run.
        :returns: DataFrame of error report which could be a filtered version if issue_id is input.
        :rtype: DataFrame
        """

        self.data_files = data_files
        self.ruleset = ruleset
        self.issue_id = issue_id

        # save independent version of data to be used in report.
        raw_data = copy.deepcopy(self.data_files)

        # run
        self.create_issue_report_df(selected_rules)
        self.select_by_id()

        # add child_id to issue location report.
        self.all_rules_issue_locs = include_issue_child(
            self.all_rules_issue_locs, raw_data
        )

    def get_rules_to_run(self, registry, selected_rules):
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
        elif error_df_lengths.max() == 4:
            # this is a return level validation rule. It has no locations attached so it is only displayed in the rule descriptions.
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

            # add a the rule's code to it's error_df
            issue_dfs_per_rule[ind]["rule_code"] = rule.code

            # temporary: add rule type to track if all types are in df.
            issue_dfs_per_rule[ind]["rule_type"] = ind

            # combine this rule's error_df with the cummulative error_df
            self.all_rules_issue_locs = pd.concat(
                [self.all_rules_issue_locs, issue_dfs_per_rule[ind]],
                ignore_index=True,
            )

            # Elements of the rule_descriptors df to explain error codes
            self.rules_broken.append(rule.code)
            self.rule_messages.append(f"{str(rule.code)} - {rule.message}")

    def create_issue_report_df(self, selected_rules):
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
        self.all_rules_issue_locs = pd.DataFrame()
        self.rules_passed = []

        self.rules_broken = []
        self.rule_messages = []
        self.la_rules_broken = []

        importlib.import_module(f"cin_validator.{self.ruleset}")

        rules_to_run = self.get_rules_to_run(registry, selected_rules)
        for rule in rules_to_run:
            # data_files = self.data_files.__deepcopy__({})
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
        la_level_rules = pd.DataFrame(self.la_rules_broken)
        if not la_level_rules.empty:
            self.rule_descriptors = pd.concat([child_level_rules, la_level_rules])
        else:
            self.rule_descriptors = child_level_rules

    def select_by_id(self):
        """
        Allows users to select reports of individual errors by ERROR_ID. Note:
        this is individual instances of errors, not rule codes.

        :param str issue_id: The ID of an individual issue, to be matched with an ERROR_ID
            from validation.
        :returns: Validation information associated with specific ERROR_ID.
        :rtype: DataFrame
        """

        if self.issue_id is not None:
            self.issue_id = tuple(self.issue_id.split(", "))
            self.all_rules_issue_locs = self.all_rules_issue_locs[
                self.all_rules_issue_locs["ERROR_ID"] == self.issue_id
            ]
        else:
            pass
