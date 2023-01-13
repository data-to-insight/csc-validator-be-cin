import importlib

import pandas as pd

from cin_validator.ingress import XMLtoCSV
from cin_validator.rule_engine import RuleContext, registry
from cin_validator.utils import DataContainerWrapper, process_date_columns


def process_data(root, as_dict=False):
    # generate tables
    data_files = XMLtoCSV(root)
    tables = [
        data_files.Header,
        data_files.ChildIdentifiers,
        data_files.ChildCharacteristics,
        data_files.ChildProtectionPlans,
        data_files.CINdetails,
        data_files.CINplanDates,
        data_files.Reviews,
        data_files.Section47,
        data_files.Assessments,
        data_files.Disabilities,
    ]
    # format all date columns in tables
    for df in tables:
        process_date_columns(df)

    # return tables
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
    def __init__(
        self, data_files=None, ruleset="rules.cin2022_23", issue_id=None
    ) -> None:

        self.data_files = data_files
        self.ruleset = ruleset
        self.issue_id = issue_id

        self.create_error_report_df()
        self.select_by_id()

    def create_error_report_df(self):
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

    def select_by_id(self):
        if self.issue_id is not None:
            self.issue_id = tuple(self.issue_id.split(", "))
            self.all_rules_issue_locs = self.all_rules_issue_locs[
                self.all_rules_issue_locs["ERROR_ID"] == self.issue_id
            ]
        else:
            pass
