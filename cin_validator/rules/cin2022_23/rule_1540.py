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
    code="1540",
    module=CINTable.ChildIdentifiers,
    message="UPN invalid (characters 5-12 not all numeric)",
    affected_fields=[UPN],
)
def validate(
    data_container: Mapping[CINTable, pd.DataFrame], rule_context: RuleContext
):
    """
    Returns indices of rows where character 5:12 of UPN contains non numerical characters.
    Does this by:
    Returning a boolean for the logic check to see is characters 5:12 contain only numerical characters.
    Using the not operator (~) to return values as false where the logic returns true (and true if there are non-numeric characters).
    Slicing df according to this criteria.
    Returns indices of the rows of this df to failing_indices."""

    df = data_container[ChildIdentifiers]

    # If <UPN> (N00001) present Characters 5-12 of <UPN> must be numeric

    #  df takes a slice of rows of df where the UPN column doesn't have Na/NaN values
    df = df.loc[df["UPN"].notna()]

    failing_indices = df[~df["UPN"].str[4:12].str.isdigit()].index

    rule_context.push_issue(table=ChildIdentifiers, field=UPN, row=failing_indices)


def test_validate():
    child_identifiers = pd.DataFrame(
        {"UPN": [pd.NA, "X000000000000", "X0000y0000000", "x0000000er00e0"]}
    )

    result = run_rule(validate, {ChildIdentifiers: child_identifiers})

    issues = list(result.issues)
    assert len(issues) == 2
    assert issues == [
        IssueLocator(CINTable.ChildIdentifiers, UPN, 2),
        IssueLocator(CINTable.ChildIdentifiers, UPN, 3),
    ]

    assert result.definition.code == "1540"
    assert result.definition.message == "UPN invalid (characters 5-12 not all numeric)"
