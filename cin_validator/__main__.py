import importlib
import xml.etree.ElementTree as ET
from pathlib import Path

import click
import pandas as pd
import pytest

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
@click.option(
    "--errorselect",
    "-e",
    default=None
)
def run_all(filename: str, ruleset, errorselect):
    # TODO detect filetype xml/csv/zip. check if the directory is a folder.
    fulltree = ET.parse(filename)
    root = fulltree.getroot()
    data_files = DataContainerWrapper(XMLtoCSV(root))

    importlib.import_module(f"cin_validator.{ruleset}")

    issue_instances = pd.DataFrame()
    all_rules_issue_locs = pd.DataFrame()
    rules_passed = []
    for rule in registry:

        try:
            ctx = RuleContext(rule)
            rule.func(data_files, ctx)
            # TODO is it wiser to split the rules according to types instead of checking the type each time a rule is run?.
            lst_types = pd.Series(
                [
                    ctx.type_zero_issues,
                    ctx.type_one_issues,
                    ctx.type_two_issues,
                    ctx.type_three_issues,
                ]
            )
            # lst_ctx is a list of lengths of all elements in lst_types respectively.
            lst_ctx = pd.Series([len(x) for x in lst_types])
            if lst_ctx.max() == 0:
                # if the rule didn't push to any of the issue accumulators
                rules_passed.append(rule.code)
            else:
                # get the rule type based on which attribute had elements pushed to it (i.e non-zero length)
                ind = lst_ctx.idxmax()

                issue_dict = {"code": rule.code, "number": lst_ctx[ind], "type": ind}
                issue_dict_df = pd.DataFrame([issue_dict])
                issue_instances = pd.concat(
                    [issue_instances, issue_dict_df], ignore_index=True
                )

                lst_types[ind]["rule_code"] = rule.code

                # temporary: add rule type to track if all types are in df.
                lst_types[ind]["rule_type"] = ind

                all_rules_issue_locs = pd.concat(
                    [all_rules_issue_locs, lst_types[ind]],
                    ignore_index=True,
                )

        except:
            print("Error with rule " + str(rule.code))


    json_issue_report = all_rules_issue_locs.to_dict(orient="records")
    print(issue_instances)
    print(all_rules_issue_locs)
    print(json_issue_report)
    if errorselect is not None:
        print(all_rules_issue_locs[all_rules_issue_locs['ERROR_ID'].notna()])
        #print(errorselect)
        #print(all_rules_issue_locs.loc[all_rules_issue_locs['ERROR_ID'] == (errorselect)])
    else:
        pass
  

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
