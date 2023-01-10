from prpc_python import RpcApp

from cin_validator import cin_validator_class as cin_class

app = RpcApp("validate_cin")


@app.call
def generate_csv():

    data_files = cin_class.process_data(
        filename="fake_data\CIN_Census_2021.xml", as_dict=True
    )
    json_data_files = {
        table_name: table_df.to_dict(orient="records")
        for table_name, table_df in data_files.items()
    }
    return json_data_files


@app.call
def cin_validate(cin_data="fake_data\CIN_Census_2021.xml", data_files=None):
    if not data_files:
        data_files = cin_class.process_data(cin_data)
    validator = cin_class.CinValidationSession(data_files, ruleset="rules.cin2022_23")
    error_report = validator.json_issue_report
    rule_defs = validator.json_rule_descriptors
    return error_report, rule_defs
