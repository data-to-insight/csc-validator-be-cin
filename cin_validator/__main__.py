import datetime
import importlib
import json
import os
import xml.etree.ElementTree as ET
from pathlib import Path

import click
import pytest

from cin_validator import cin_validator


@click.group()
def cli():
    pass


@cli.command(name="list")
@click.option(
    "--ruleset",
    "-r",
    default="cin2022_23",
    help="Which ruleset to use, e.g. cin2022_23",
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
    module = importlib.import_module(f"cin_validator.rules.{ruleset}")
    ruleset_registry = getattr(module, "registry")
    for _, rule in ruleset_registry.items():
        click.echo(f"{rule.code}\t{rule.message} ({rule.rule_type.name})")


@cli.command(name="run")
@click.argument("filename", type=click.File("rt"), required=True)
@click.option(
    "--ruleset",
    "-r",
    default="cin2022_23",
    help="Which ruleset to use, e.g. cin2022_23",
)
@click.option("--select", "-s", default=None)
@click.option("--output/--no_output", "-o/-no", default=False)
def run_all(filename: str, ruleset, select, output):
    """
    Used to run all of a set of validation rules on input data.

    CLI command:
    python -m cin_validator run <filepath_to_data>

    Can be used to validate data, via the command line interface, for a given rule set.
    Runs with the cin2022_23 ruleset as standard.

    :param str filename: Refers to the filepath of data to be validated.
    :param str ruleset: The folder name of the validation rules to run input data against.
    :param select: specify the rules that should be run. CLI works with a single string only.
    :param bool output: If true, produces csv output of error report, if False (default)
        does not.
    :returns: DataFrame report of errors using selected validation rules, also output as
        JSON when output is True.
    :rtype: DataFrame, JSON
    """

    fulltree = ET.parse(filename)
    root = fulltree.getroot()

    raw_data = cin_validator.convert_data(root)
    data_files = cin_validator.process_data(raw_data)

    # get rules based on specified year.
    module = importlib.import_module(f"cin_validator.rules.{ruleset}")
    ruleset_registry = getattr(module, "registry")

    validator = cin_validator.CinValidator(
        data_files, ruleset_registry, selected_rules=select
    )

    full_issue_df = validator.full_issue_df

    if output:
        validator.user_report.to_csv("user_report.csv")

    # click.echo(full_issue_df)
    # click.echo(validator.multichild_issues)
    click.echo(validator.data_files["Assessments"])


@cli.command(name="test")
@click.argument("rule", required=False)
@click.option(
    "--ruleset",
    "-r",
    default="cin2022_23",
    help="Which ruleset to use, e.g. cin2022_23",
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
        (defaults to cin2022_23).
    :returns: Pytest output in terminal of rules passing and failing.
    :rtype: Pytest output in terminal.
    """

    module = importlib.import_module(f"cin_validator.rules.{ruleset}")
    module_folder = Path(module.__file__).parent

    ruleset_registry = getattr(module, "registry")

    if rule:
        rule = str(rule)
        # when rule code is specified, test specific rule.
        rule_def = ruleset_registry.get(rule)
        if not rule_def:
            # if the get returns a <NoneType>
            click.secho(f"Rule {rule} not found.", err=True, fg="red")
            return 1
        test_files = [os.path.join(module_folder, f"rule_{rule}.py")]
    else:
        # else test all rules.
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

        cin_tables_dict = cin_validator.convert_data(root)
        for k, v in cin_tables_dict.items():
            filepath = Path(f"output_csvs/{k}.csv")
            filepath.parent.mkdir(parents=True, exist_ok=True)
            v.to_csv(filepath)
    else:
        click.echo(f"{filename} can't be found, have you entered it correctly?")


@cli.command(name="timer")
@click.argument("filepath", type=str, required=True)
def timer(filepath):
    st = datetime.datetime.now()
    os.system(f"python -m cin_validator run {filepath}")
    et = datetime.datetime.now()
    time_elapsed = et - st
    click.echo(f"Time to run: {time_elapsed}")


if __name__ == "__main__":
    cli()
