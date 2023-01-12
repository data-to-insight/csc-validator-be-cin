import importlib
import xml.etree.ElementTree as ET
from pathlib import Path

import click
import pytest

from cin_validator import cin_validator_class as cin_class
from cin_validator.ingress import XMLtoCSV
from cin_validator.rule_engine import registry


@click.group()
def cli():
    pass


@cli.command(name="list")
@click.option(
    "--ruleset",
    "-r",
    default="rules.cin2022_23",
    help="Which ruleset to use, e.g. rules.cin2022_23",
)
def list_cmd(ruleset):
    """List all rules in a ruleset.

    :param str ruleset: The name of a CIN validation ruleset, used to select rules for validating data.
    :returns: A list of validation rules in the given ruleset.
    :rtype: list
    """
    importlib.import_module(f"cin_validator.{ruleset}")
    for rule in registry:
        click.echo(f"{rule.code}\t{rule.message} ({rule.rule_type.name})")


@cli.command()
@click.argument("filename", type=click.File("rt"), required=True)
@click.option(
    "--ruleset",
    "-r",
    default="rules.cin2022_23",
    help="Which ruleset to use, e.g. rules.cin2022_23",
)
@click.option("--issue_id", "-e", default=None)
@click.option("--output/--no_output", "-o/-no", default=False)
def run_all(filename: str, ruleset, issue_id, output):
    """Used to run all of a set of validation rules on input data.

    CLI command:
    python -m cin_validator run-all <filepath_to_data>
    can be used to validate data via the data for a given rule set.
    Runs with the cin2022_23 ruleset as standard.

    :param str filename: Refers to the filepath of data to be validated.
    :param str ruleset: The ruleset of validation rules to run input data against.
    :param str issue_id: Can be used to select an individual instance of an
        error, using indivdual ERROR_ID
    :param bool output: If true, produces JSON error report output, if False (default)
        does not.
    :returns: DataFrame report of errors using selected validation rules, also output as
        JSON when output is True.
    :rtype: DataFrame, JSON
    """
    data_files = cin_class.process_data(filename)
    validator = cin_class.CinValidationSession(data_files, ruleset, issue_id)

    issue_instances = validator.issue_instances
    all_rules_issue_locs = validator.all_rules_issue_locs

    if output:
        error_report = validator.json_issue_report
        rule_defs = validator.json_rule_descriptors

        # generating sample files for the frontend.
        # TODO. when frontend dev is complete, change this to generate csv.
        import json

        with open("rule_defs.json", "w") as f:
            json.dump(rule_defs, f)

        with open("issue_report.json", "w") as f:
            json.dump(error_report, f)

    # print(issue_instances)
    # print(all_rules_issue_locs)
    # print(validator.rule_descriptors)


@cli.command(name="test")
@click.argument("rule", type=int, required=False)
@click.option(
    "--ruleset",
    "-r",
    default="rules.cin2022_23",
    help="Which ruleset to use, e.g. rules.cin2022_23",
)
def test_cmd(rule, ruleset):
    """Test all (or individual) rules in a ruleset.

    Allows use of the CLI to test a ruleset or individual rules against the
    pytest written in each of their files. Useful for bugfixing. Defaults
    to the cin2022_23 ruleset.

    Can be called to test all rules using:
    python -m cin_validator test
    To test individual rules:
    python -m cin_vaslidator <rulecode>
    For example:
    python -m cin_validator test 8875

    :param str rule: Used to specify an individual rule to test.
    :param str ruleset: Use to give the name of a set of validation rules to test.
    :returns: Pytest output in terminal of rules passing and failing.
    :rtype: Pytest output in terminal.
    """
    module = importlib.import_module(f"cin_validator.{ruleset}")
    module_folder = Path(module.__file__).parent

    if rule:
        rule_def = registry.get(rule)
        if not rule_def:
            click.secho(f"Rule {rule} not found.", err=True, fg="red")
            return 1
        test_files = [registry.get(rule).code_module.__file__]
    else:
        test_files = [
            str(p.absolute())
            for p in module_folder.glob("*.py")
            if p.stem != "__init__"
        ]
    pytest.main(test_files)


@cli.command(name="xmltocsv")
@click.argument("filename", type=click.Path(), required=True)
def cli_converter(filename: str):
    """Converts XML to CSV at selected filepath, does not require
    XML to be validated against validation rules and does not validate
    agaisnt rules. Called using:
    python -m cin_validator xmltocsv <filepath>

    :param str filename: Filename (including filepath) of XML file to convert to CSV.
    :returns: CSV of XML input into output_csvs directory (which will be created
        if it doesn't already exist).
    :rtype: CSVs (multiple).

    """
    if Path(filename).exists():
        fulltree = ET.parse(filename)
        root = fulltree.getroot()
        data_files = XMLtoCSV(root)
        cin_tables_dict = {
            "Header": data_files.Header,
            "ChildIdentifiers": data_files.ChildIdentifiers,
            "ChildCharacteristics": data_files.ChildCharacteristics,
            "ChildProtectionPlans": data_files.ChildProtectionPlans,
            "CINdetails": data_files.CINdetails,
            "CINplanDates": data_files.CINplanDates,
            "Reviews": data_files.Reviews,
            "Section47": data_files.Section47,
        }
        for k, v in cin_tables_dict.items():
            #  TODO output CSVs as a zip file
            filepath = Path(f"output_csvs/{k}.csv")
            filepath.parent.mkdir(parents=True, exist_ok=True)
            v.to_csv(filepath)
    else:
        click.echo(f"{filename} can't be found, have you entered it correctly?")


if __name__ == "__main__":
    cli()
