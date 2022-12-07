import importlib
import xml.etree.ElementTree as ET

import pandas as pd

from cin_validator.ingress import XMLtoCSV
from cin_validator.rule_engine import RuleContext, registry
from cin_validator.utils import DataContainerWrapper


class CinValidationSession:
    def __init__(self, filename, ruleset, issue_id=None) -> None:
        # TODO detect filetype xml/csv/zip. check if the directory is a folder.
        fulltree = ET.parse(filename)
        root = fulltree.getroot()
        self.data_files_obj = DataContainerWrapper(XMLtoCSV(root))

        self.ruleset = ruleset
        self.issue_id = issue_id

        self.create_error_report_df()
        self.create_json_report()
        self.select_by_id()

    def create_error_report_df(self):
        self.issue_instances = pd.DataFrame()
        self.all_rules_issue_locs = pd.DataFrame()
        self.rules_passed = []
        self.rules_broken = []
        self.rule_messages = []

        importlib.import_module(f"cin_validator.{self.ruleset}")

        for rule in registry:
            data_files = self.data_files_obj.__deepcopy__({})
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
                    ]
                )
                # error_df_lengths is a list of lengths of all elements in issue_dfs_per_rule respectively.
                error_df_lengths = pd.Series([len(x) for x in issue_dfs_per_rule])
                if error_df_lengths.max() == 0:
                    # if the rule didn't push to any of the issue accumulators, then it didn't find any issues in the file.
                    self.rules_passed.append(rule.code)
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
                    self.rule_messages.append(rule.message)

            except Exception as e:
                print(f"Error with rule {rule.code}: {type(e).__name__}, {e}")

        # df of all broken rule codes and related error messages.
        self.rule_descriptors = pd.DataFrame(
            {"Rule code": self.rules_broken, "Rule Message": self.rule_messages}
        )

    def create_json_report(self):
        """Creates JSONs of error report and rule descriptors dfs."""
        self.json_issue_report = self.all_rules_issue_locs.to_dict(orient="records")
        self.json_rule_descriptors = self.rule_descriptors.to_dict(orient="records")

    def select_by_id(self):
        if self.issue_id is not None:
            self.issue_id = tuple(self.issue_id.split(", "))
            self.all_rules_issue_locs = self.all_rules_issue_locs[
                self.all_rules_issue_locs["ERROR_ID"] == self.issue_id
            ]
        else:
            pass
