import importlib
import xml.etree.ElementTree as ET

import pandas as pd
from prpc_python import RpcApp

from cin_validator import cin_validator_class as cin_class

app = RpcApp("validate_cin")


@app.call
def get_rules(ruleset="rules.cin2022_23"):
    """
    :param str ruleset: validation ruleset according to year published.
    :return rules_df: available rule codes and definitions according to chosen ruleset.
    """
    from cin_validator.rule_engine import registry

    importlib.import_module(f"cin_validator.{ruleset}")

    rules = []
    for rule in registry:
        rules.append(
            {
                "code": str(rule.code),
                "description": str(rule.code) + " - " + str(rule.message),
            }
        )

    # dataframe of rule_definitions
    rules_df = pd.DataFrame(rules)

    json_rules_df = rules_df.to_json(orient="records")
    return json_rules_df


@app.call
def generate_tables(cin_data):
    """
    :param cin_data: file reference to a CIN XML file
    :return json_data_files:  a dictionary of dataframes that has been converted to json.
    """
    filetext = cin_data.read().decode("utf-8")
    root = ET.fromstring(filetext)

    data_files = cin_class.convert_data(root)

    # make data json-serialisable
    json_data_files = {
        table_name: table_df.to_json(orient="records")
        for table_name, table_df in data_files.items()
    }

    return json_data_files


@app.call
def cin_validate(cin_data, selected_rules=None, ruleset="rules.cin2022_23"):
    """
    :param file-ref cin_data: file reference to a CIN XML file
    :param list selected_rules: array of rules the user has chosen. consists of rule codes as strings.
    :param ruleset: rule pack that should be run. cin2022_23 is for the year 2022

    :return issue_report: issue locations in the data.
    :return rule_defs: rule codes and descriptions of the rules that triggers issues in the data.
    """

    filetext = cin_data.read().decode("utf-8")
    root = ET.fromstring(filetext)

    # fulltree = ET.parse("fake_data\\fake_CIN_data.xml")
    # root = fulltree.getroot()

    raw_data = cin_class.convert_data(root)

    # Send string-format data to the frontend.
    json_data_files = {
        table_name: table_df.to_json(orient="records")
        for table_name, table_df in raw_data.items()
    }

    # Convert date columns to datetime format to enable comparison in rules.
    data_files = cin_class.process_data(raw_data)

    # run validation
    validator = cin_class.CinValidationSession(
        data_files, ruleset, selected_rules=selected_rules
    )

    # make return data json-serialisable

    # what the frontend will display
    issue_report = validator.full_issue_df.to_json(orient="records")
    rule_defs = validator.rule_descriptors.to_json(orient="records")

    # what the user will download
    user_report = validator.user_report.to_json(orient="records")

    return issue_report, rule_defs, json_data_files, user_report
