import importlib
import xml.etree.ElementTree as ET
from pathlib import Path

import click
import pytest

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
@click.option("--output/--no_output", "-o/-no", default=False)
def run_all(filename: str, ruleset, issue_id, output):

    fulltree = ET.parse(filename)
    root = fulltree.getroot()

    data_files = cin_class.process_data(root)
    validator = cin_class.CinValidationSession(data_files, ruleset, issue_id)

    issue_instances = validator.issue_instances
    all_rules_issue_locs = validator.all_rules_issue_locs

    if output:
        error_report = validator.all_rules_issue_locs.to_json(orient="records")
        rule_defs = validator.rule_descriptors.to_json(orient="records")

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

        cin_tables_dict = cin_class.process_data(root, as_dict=True)
        for k, v in cin_tables_dict.items():
            #  TODO output CSVs as a zip file
            filepath = Path(f"output_csvs/{k}.csv")
            filepath.parent.mkdir(parents=True, exist_ok=True)
            v.to_csv(filepath)
    else:
        click.echo(f"{filename} can't be found, have you entered it correctly?")


if __name__ == "__main__":
    cli()
