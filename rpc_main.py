import datetime
import json
import logging
import xml.etree.ElementTree as ET
from typing import Optional

from prpc_python import RpcApp

from cin_validator import cin_validator
from cin_validator.rules.ruleset_utils import get_year_ruleset

logger = logging.getLogger(__name__)
handler = logging.FileHandler(
    datetime.datetime.now().strftime("lac validator --%d-%m-%Y %H.%M.%S.log")
)

f_format = logging.Formatter("%(asctime)s - %(levelname)s - % (message)s")
handler.setFormatter(f_format)
logger.addHandler(handler)

app = RpcApp("validate_cin")


@app.call
def get_rules(collection_year: str) -> str:
    """
    :param str collection_year: validation year e.g "2023" for 2022/2023 validation rules.
    :return rules_df: available rule codes and definitions according to chosen ruleset.
    """
    ruleset_registry = get_year_ruleset(collection_year)

    rules = []
    for _, rule in ruleset_registry.items():
        rules.append(
            {
                "code": str(rule.code),
                "description": str(rule.code) + " - " + str(rule.message),
            }
        )

    return json.dumps(rules)


@app.call
def generate_tables(cin_data: dict) -> dict[str, dict]:
    """
    :param cin_data: files uploaded by user mapped to the field where files were uploaded.
    :return cin_data_tables:  a dictionary of dataframes that has been converted to json.
    """
    # Only a single XML file representing the current year is accepted as an input by the tool.
    cin_data_file = cin_data["This year"][0]
    filetext = cin_data_file.read().decode("utf-8")
    root = ET.fromstring(filetext)

    data_files = cin_validator.convert_data(root)

    # make data json-serialisable
    cin_data_tables = {
        table_name: table_df.to_json(orient="records")
        for table_name, table_df in data_files.items()
    }

    return cin_data_tables


@app.call
def cin_validate(
    cin_data: dict,
    file_metadata: dict,
    selected_rules: Optional[list[str]] = None,
):
    """
    :param cin_data: eys are table names and values are CIN csv files.
    :param file_metadata: contains collection year and local authority as strings.
    :param selected_rules: array of rules the user has chosen. consists of rule codes as strings.

    :return issue_report: issue locations in the data.
    :return rule_defs: codes and descriptions of the rules that triggers issues in the data.
    """
    cin_data_file = cin_data["This year"][0]
    filetext = cin_data_file.read().decode("utf-8")
    root = ET.fromstring(filetext)

    # fulltree = ET.parse("fake_data\\fake_CIN_data.xml")
    # root = fulltree.getroot()

    raw_data = cin_validator.convert_data(root)

    # Send string-format data to the frontend.
    cin_data_tables = {
        table_name: table_df.to_json(orient="records")
        for table_name, table_df in raw_data.items()
    }

    # Convert date columns to datetime format to enable comparison in rules.
    data_files = cin_validator.process_data(raw_data)
    # get rules to run based on specified year.
    ruleset_registry = get_year_ruleset(file_metadata["collectionYear"])

    # run validation
    validator = cin_validator.CinValidator(data_files, ruleset_registry, selected_rules)

    # make return data json-serialisable

    # what the frontend will display
    issue_report = validator.full_issue_df.to_json(orient="records")
    multichild_issues = validator.multichild_issues.to_json(orient="records")

    # what the user will download
    user_report = validator.user_report.to_json(orient="records")

    validation_results = {
        "issue_locations": [issue_report],
        "multichild_issues": [multichild_issues],
        "data_tables": [cin_data_tables],
        "user_report": [user_report],
    }
    return validation_results
