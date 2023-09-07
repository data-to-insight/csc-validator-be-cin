from typing import Mapping

import pandas as pd

from cin_validator.rule_engine import (
    CINTable,
    IssueLocator,
    RuleContext,
    rule_definition,
)
from cin_validator.test_engine import run_rule

ChildIdentifiers = CINTable.ChildIdentifiers
UPN = ChildIdentifiers.UPN


@rule_definition(
    code="1550",
    module=CINTable.ChildIdentifiers,
    message="UPN invalid (character 13 not a recognised value)",
    affected_fields=[UPN],
)
def validate(
    data_container: Mapping[CINTable, pd.DataFrame], rule_context: RuleContext
):
    df = data_container[ChildIdentifiers]

    # if <UPN> (N00001)) present Character 13 of <UPN> must be numeric or A-Z omitting I, O and S
    # Confirm length and value present
    df2 = df[(df["UPN"].str.len() == 13) & df["UPN"].notna()]

    # Valid characters
    valid = [
        "A",
        "B",
        "C",
        "D",
        "E",
        "F",
        "G",
        "H",
        "J",
        "K",
        "L",
        "M",
        "N",
        "P",
        "Q",
        "R",
        "T",
        "U",
        "V",
        "W",
        "Y",
        "X",
        "Z",
        "0",
        "1",
        "2",
        "3",
        "4",
        "5",
        "6",
        "7",
        "8",
        "9",
    ]

    failing_indices = df2[~df2["UPN"].str[12].isin(valid)].index

    rule_context.push_issue(table=ChildIdentifiers, field=UPN, row=failing_indices)


def test_validate():
    child_identifiers = pd.DataFrame(
        [["1234567891234"], ["123456789123I"], ["123456789123O"]], columns=[UPN]
    )

    result = run_rule(validate, {ChildIdentifiers: child_identifiers})

    issues = list(result.issues)
    assert len(issues) == 2
    assert issues == [
        IssueLocator(CINTable.ChildIdentifiers, UPN, 1),
        IssueLocator(CINTable.ChildIdentifiers, UPN, 2),
    ]

    assert result.definition.code == "1550"
    assert (
        result.definition.message == "UPN invalid (character 13 not a recognised value)"
    )
