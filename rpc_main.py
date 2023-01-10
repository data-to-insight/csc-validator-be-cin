from prpc_python import RpcApp

from cin_validator import cin_validator_class as cin_class

app = RpcApp("validate_cin")


@app.call
def generate_tables(cin_data):
    """
    :param cin_data: file reference to a CIN XML file
    :return json_data_files:  a dictionary of dataframes that has been converted to json.
    """

    data_files = cin_class.process_data(cin_data, as_dict=True)

    # make data json-serialisable
    json_data_files = {
        table_name: table_df.to_dict(orient="records")
        for table_name, table_df in data_files.items()
    }

    return json_data_files


@app.call
def cin_validate(cin_data, ruleset="rules.cin2022_23"):
    """
    :param cin_data: file reference to a CIN XML file
    :param ruleset: rule pack that should be run. cin2022_23 is for the year 2022

    :return issue_report: issue locations in the data.
    :return rule_defs: rule codes and descriptions of the rules that triggers issues in the data.
    """

    data_files = cin_class.process_data(cin_data)
    validator = cin_class.CinValidationSession(data_files, ruleset)
    raw_data = cin_class.process_data(cin_data, as_dict=True)

    # make return data json-serialisable
    issue_report = validator.json_issue_report
    rule_defs = validator.json_rule_descriptors
    json_data_files = {
        table_name: table_df.to_dict(orient="records")
        for table_name, table_df in raw_data.items()
    }

    return issue_report, rule_defs, json_data_files
