import importlib
import xml.etree.ElementTree as ET

import pandas as pd

from cin_validator.ingress import XMLtoCSV
from cin_validator.rule_engine import RuleContext, registry
from cin_validator.utils import DataContainerWrapper


def process_data(filename, as_dict=False):
    """Takes input data and processes it for validation.

    This function takes input XML data, and uses ElementTree, and the custom class
    XMLtoCSV to process the data into tables for validation. Returning as a dict
    or DataContainerWrapper object. Returns as dict for front end purposes and object
    for CLI interface.

    :param XML filename: XML files passed from either the front end or the CLI.
    :returns: Data files as object or dict of dfs for validation.
    :rtype: dictionary or DataContainerWrapper object
    """
    fulltree = ET.parse(filename)
    root = fulltree.getroot()
    data_files = XMLtoCSV(root)
    if as_dict:
        cin_tables_dict = {
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
        return cin_tables_dict
    else:
        data_files_obj = DataContainerWrapper(data_files)
        return data_files_obj


class CinValidationSession:
    """A class to contain the process of CIN validation, including producing error reports,
    and formatting error reports into JSONs for the FE.

    :param data_files: Data files for validation, either a DtaContainerWrapper object, or a
        dicitonary of DataFrames.
    :param directory ruleset: The directory containing the validation rules to be run in a particular
        validation session.
    :param str issue_id: ID of individual errors to be selected for viewing using
        select_by_id method (Error IDs, not rule codes).
    """

    def __init__(
        self, data_files=None, ruleset="rules.cin2022_23", issue_id=None
    ) -> None:
        """Constructor method"""
        self.data_files = data_files
        self.ruleset = ruleset
        self.issue_id = issue_id

        self.create_error_report_df()
        self.create_json_report()
        self.select_by_id()

    def create_error_report_df(self):
        """Creates report of errors found when validating CIN data input to
        the tool.

        This function takes the errors reported by individual validation rule functions,
        including table, field, and index locations of errors. It runs through every rule
        in the registry and:

        >Creates lists of rules passed, broken, and relevant messages.
        >Returns a dataframe of issue instances for broken validation rules.
        >Returns a dictionary of all rules codes and relevant messages.

        :param DataFrame issue_instances: DataFrame of instances of issues found in  validation
        :param DataFrame all_rules_issue_locs: DataFrame of locations of issues found in validation.
        :param list rules_broken: An empty list of rules broken, which is populated with the list
            of rules failing validation upon validation.
        :param list la_rules_broken: An empty list of LA level rules broken, which is populated with the list
            of rules failing validation upon validation.
        :param list rules_passed: An empty list of rules passed, which is populated with the list
            of rules failing validation upon validation.
        :param list rules_passed: An empty list of rules passed, populated with rules with no validation errors
            upon validation.
        :returns: DataFrame of instances and locations of validation rule violations from data input via FE or CLI.
        :rtype: DataFrame
        :raises: Errors with rules that raise errors when validating data.
        """
        self.issue_instances = pd.DataFrame()
        self.all_rules_issue_locs = pd.DataFrame()
        self.rules_passed = []

        self.rules_broken = []
        self.rule_messages = []
        self.la_rules_broken = []

        importlib.import_module(f"cin_validator.{self.ruleset}")

        for rule in registry:
            data_files = self.data_files.__deepcopy__({})
            try:
                ctx = RuleContext(rule)
                rule.func(data_files, ctx)
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

            except Exception as e:
                print(f"Error with rule {rule.code}: {type(e).__name__}, {e}")

        # df of all broken rule codes and related error messages.
        child_level_rules = pd.DataFrame(
            {"Rule code": self.rules_broken, "Rule Message": self.rule_messages}
        )
        la_level_rules = pd.DataFrame(self.la_rules_broken)
        if not la_level_rules.empty:
            self.rule_descriptors = pd.concat([child_level_rules, la_level_rules])
        else:
            self.rule_descriptors = child_level_rules

    def create_json_report(self):
        """Creates JSONs of error report and rule descriptors dfs.

        Flattens the issue reports and rule descriptors reports made in
        create_error_report_df into JSONs to be sent to the FE.

        :param CinValidationSession object self:
        :returns: JSONs of CIN validation error reports.
        :rtype: JSON
        """
        self.json_issue_report = self.all_rules_issue_locs.to_dict(orient="records")
        self.json_rule_descriptors = self.rule_descriptors.to_dict(orient="records")

    def select_by_id(self):
        """Allows users to select reports of individual errors by ERROR_ID. Note:
        this is individual instances of errors, not rule codes.

        :param str issue_id: The ID of an individual issue, to be matched with an ERROR_ID
            from validation.
        :returns: Validation information associated with specific ERROR_ID.
        :rtype: str
        """
        if self.issue_id is not None:
            self.issue_id = tuple(self.issue_id.split(", "))
            self.all_rules_issue_locs = self.all_rules_issue_locs[
                self.all_rules_issue_locs["ERROR_ID"] == self.issue_id
            ]
        else:
            pass
