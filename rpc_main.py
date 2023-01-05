from prpc_python import RpcApp

from cin_validator import cin_validator_class as cin_class

app = RpcApp("validate_cin")

data_files = None


@app.call
def generate_csv():
    global data_files
    data_files = cin_class.process_data(
        filename="/workspaces/CIN-validator/fake_data/CIN_Census_2021.xml",
    )
    return data_files


@app.call
def cin_validate():
    global data_files
    if data_files:
        data_files = data_files
    else:
        data_files = cin_class.process_data(
            filename="/workspaces/CIN-validator/fake_data/CIN_Census_2021.xml"
        )

    validator = cin_class.CinValidationSession(data_files, ruleset="rules.cin2022_23")

    error_report = validator.json_issue_report
    rule_defs = validator.json_rule_descriptors
    return error_report, rule_defs
