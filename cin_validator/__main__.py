import importlib
import xml.etree.ElementTree as ET
from pathlib import Path

import click
import pandas as pd
import pytest

from cin_validator.cin_validator_class import CinValidationSession
from cin_validator.ingress import XMLtoCSV
from cin_validator.rule_engine import RuleContext, registry
from cin_validator.utils import DataContainerWrapper


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
    """List all rules in a ruleset"""
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
def run_all(filename: str, ruleset, issue_id):

    validator = CinValidationSession(filename, ruleset, issue_id)

    issue_instances = validator.issue_instances
    all_rules_issue_locs = validator.all_rules_issue_locs

    print(issue_instances)
    print(all_rules_issue_locs)

    # Allows selection of error by ERROR_ID,
    # converts errorselect argument to tuple to do the slice.
    # if issue_id is not None:
    #     issue_id = tuple(map(str, issue_id.split(", ")))
    #     print(all_rules_issue_locs[all_rules_issue_locs["ERROR_ID"] == issue_id])
    # else:
    #     pass


@cli.command(name="test")
@click.argument("rule", type=int, required=False)
@click.option(
    "--ruleset",
    "-r",
    default="rules.cin2022_23",
    help="Which ruleset to use, e.g. rules.cin2022_23",
)
def test_cmd(rule, ruleset):
    """Test all (or individual) rules in a ruleset"""
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
    """Converts XML to CSV at selected filepath"""
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
