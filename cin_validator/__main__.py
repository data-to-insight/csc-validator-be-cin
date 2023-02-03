import importlib
import xml.etree.ElementTree as ET
from pathlib import Path

import click
import pytest

import datetime
import os

from cin_validator import cin_validator_class as cin_class
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
    """
    List all rules in a ruleset.

    Call using:
    python -m cin_validator list

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
@click.option("--select", "-s", default=None)
@click.option("--output/--no_output", "-o/-no", default=False)
def run_all(filename: str, ruleset, issue_id, select, output):
    """
    Used to run all of a set of validation rules on input data.

    CLI command:
    python -m cin_validator run-all <filepath_to_data>

    Can be used to validate data via the data for a given rule set.
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

    fulltree = ET.parse(filename)
    root = fulltree.getroot()

    raw_data = cin_class.convert_data(root)
    data_files = cin_class.process_data(raw_data)

    validator = cin_class.CinValidationSession(
        data_files, ruleset, issue_id, selected_rules=select
    )

    issue_instances = validator.issue_instances
    full_issue_df = validator.full_issue_df

    if output:
        # TODO when dict of dfs can be passed into this class, run include_issue_child on issue_report
        issue_report = validator.full_issue_df.to_json(orient="records")
        rule_defs = validator.rule_descriptors.to_json(orient="records")

        # generating sample files for the frontend.
        # TODO. when frontend dev is complete, change this to generate csv.
        import json

        with open("rule_defs.json", "w") as f:
            json.dump(rule_defs, f)

        with open("issue_report.json", "w") as f:
            json.dump(issue_report, f)

    print(issue_instances)
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
    """
    Test all (or individual) rules in a ruleset. Note: tests the code
    for the rule, this is not used for validating data.

    Allows use of the CLI to test a ruleset or individual rules against the
    pytest written in each of their files. Useful for bugfixing. Defaults
    to the cin2022_23 ruleset.

    Can be called to test all rules using:
    python -m cin_validator test
    To test individual rules:
    python -m cin_validator <rulecode>
    For example:
    python -m cin_validator test 8875

    :param str rule: Used to specify an individual rule to test.
    :param str ruleset: Use to give the name of a set of validation rules to test
        (defaults to rules.cin2022_23).
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
    """
    Converts XML to CSV at selected filepath. Does not require XML to be validated against validation rules and does not validate against rules.
    Called using:
    python -m cin_validator xmltocsv <filepath>

    :param str filename: filename (or path) of XML file to convert to CSV.
    :returns: CSV of XML input into output_csvs directory (which will be created
        if it doesn't already exist).
    :rtype: CSVs (multiple).

    """
    if Path(filename).exists():
        fulltree = ET.parse(filename)
        root = fulltree.getroot()

        cin_tables_dict = cin_class.process_data(root)
        for k, v in cin_tables_dict.items():
            #  TODO output CSVs as a zip file
            filepath = Path(f"output_csvs/{k}.csv")
            filepath.parent.mkdir(parents=True, exist_ok=True)
            v.to_csv(filepath)
    else:
        click.echo(f"{filename} can't be found, have you entered it correctly?")


@cli.command(name="timer")
def timer():
    st = datetime.datetime.now()
    os.system(
        "python -m cin_validator run-all /workspaces/CIN-validator/fake_data/fake_CIN_data.xml"
    )
    et = datetime.datetime.now()
    time_elapsed = et - st
    print(f"Time to run: {time_elapsed}")


if __name__ == "__main__":
    cli()
