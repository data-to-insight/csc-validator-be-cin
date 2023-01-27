import xml.etree.ElementTree as ET

from prpc_python import RpcApp

from cin_validator import cin_validator_class as cin_class

app = RpcApp("validate_cin")


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

    raw_data = cin_class.convert_data(root)
    json_data_files = {
        table_name: table_df.to_json(orient="records")
        for table_name, table_df in raw_data.items()
    }

    # format date columns
    data_files = cin_class.process_data(raw_data)
    # run validation
    validator = cin_class.CinValidationSession(
        data_files, ruleset, selected_rules=selected_rules
    )
    issue_df = validator.full_issue_df

    # make return data json-serialisable
    issue_report = issue_df.to_json(orient="records")
    rule_defs = validator.rule_descriptors.to_json(orient="records")

    return issue_report, rule_defs, json_data_files
