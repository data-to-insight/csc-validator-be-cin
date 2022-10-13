from typing import Mapping

import pandas as pd

from cin_validator.rule_engine import rule_definition, CINTable, RuleContext
from cin_validator.rule_engine import IssueLocator
from cin_validator.test_engine import run_rule

ChildIdentifiers = CINTable.ChildIdentifiers
Header = CINTable.Header
PersonDeathDate = ChildIdentifiers.PersonDeathDate

# define characteristics of rule
@rule_definition(
    code="8545Q",
    module=CINTable.ChildIdentifiers,
    message="Please check: Child's date of death should be within the census year",
    affected_fields=[PersonDeathDate],
)
def validate(
    data_container: Mapping[CINTable, pd.DataFrame], rule_context: RuleContext
):
    df = data_container[ChildIdentifiers]
    df_ref = data_container[Header]
 
    """
    If present, <PersonDeathDate> (N00108) must be within [Period_of_Census]
    """

    df = df[["PersonDeathDate"]]
    df["PersonDeathDate"] = pd.to_datetime(df["PersonDeathDate"], format=r"%Y-%m-%d", errors="coerce")
    #Death date must not be null, invalid text dates are made null in the line above
    df = df[df["PersonDeathDate"].notna()]
    #Create a key column to join
    df["Key"] = 0

    df_ref = df_ref[["ReferenceDate"]]
    df_ref["YearEnd"] = pd.to_datetime(df_ref["ReferenceDate"], format=r"%Y-%m-%d", errors="coerce") #equals 31/03/2023  (for example, year can be different)
    df_ref["YearStart"] = df_ref["YearEnd"] - pd.DateOffset(years=1) + pd.DateOffset(days=1)         #equals 01/04/2022  (for example, year can be different)
    #Create a key column to join
    df_ref["Key"] = 0

    df = df.merge(df_ref, on="Key")

    #DeathDate isn't in the financial year
    df = df[~((df["PersonDeathDate"] >= df["YearStart"]) & (df["PersonDeathDate"] <= df["YearEnd"]))]

    failing_indices = df.index

    rule_context.push_issue(
        table=ChildIdentifiers, field=PersonDeathDate, row=failing_indices
    )


def test_validate():

    PDeath = ["2022-04-25", "2022-03-01", "2022-12-25", "2021-04-27", "2021-11-21", "2021-08-20", "2020-04-17", "1666-55-55", pd.NA]

    fake_ident = pd.DataFrame({"PersonDeathDate": PDeath})
    fake_head = pd.DataFrame({"ReferenceDate": ["2022-03-31"]})

    result = run_rule(validate, {ChildIdentifiers: fake_ident, Header: fake_head})

    issues = list(result.issues)

    assert len(issues) == 3

    assert issues == [
        IssueLocator(CINTable.ChildIdentifiers, PersonDeathDate, 0),
        IssueLocator(CINTable.ChildIdentifiers, PersonDeathDate, 2),
        IssueLocator(CINTable.ChildIdentifiers, PersonDeathDate, 6),
    ]

    assert result.definition.code == "8545Q"
    assert (
        result.definition.message
        == "Please check: Child's date of death should be within the census year"
    )