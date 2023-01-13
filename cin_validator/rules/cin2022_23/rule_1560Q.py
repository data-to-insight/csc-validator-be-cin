from typing import Mapping

import pandas as pd

from cin_validator.rule_engine import (
    CINTable,
    IssueLocator,
    RuleContext,
    RuleType,
    rule_definition,
)
from cin_validator.test_engine import run_rule

ChildIdentifiers = CINTable.ChildIdentifiers
UPN = ChildIdentifiers.UPN


@rule_definition(
    code="1560Q",
    module=CINTable.ChildIdentifiers,
    rule_type=RuleType.QUERY,
    message="Please check: Former UPN wrongly formatted",
    affected_fields=[UPN],
)
def validate(
    data_container: Mapping[CINTable, pd.DataFrame], rule_context: RuleContext
):
    df = data_container[ChildIdentifiers]

    # <FormerUPN> (N00002) where present should be in the correct format, as specified in the data table

    #  filter rows of df where the UPN column doesn't have Na/NaN values
    df = df.loc[df["UPN"].notna()]

    # Flag locations where
    # FormerUPN is not 13 characters long
    check_length = df["UPN"].str.len() != 13
    # FormerUPN does not contain a full digit between edges.
    digit_within = ~df["UPN"].str[1:-1].str.isdigit()
    # FormerUPN's edges are not characters of th alphabet
    check_edges = (~df["UPN"].str[0].str.isalpha()) | (~df["UPN"].str[-1].str.isalpha())

    failing_indices = df[check_length | digit_within | check_edges].index

    rule_context.push_issue(table=ChildIdentifiers, field=UPN, row=failing_indices)


def test_validate():
    child_identifiers = pd.DataFrame(
        [
            {"UPN": pd.NA},  # 0 ignore
            {"UPN": "X98765432123B"},  # 1 pass
            {"UPN": "X0000y000000K"},  # 2 fail non-alphabet within
            {"UPN": "X9872123B"},  # 3 wrong length
        ]
    )

    result = run_rule(validate, {ChildIdentifiers: child_identifiers})

    issues = list(result.issues)
    assert len(issues) == 2
    assert issues == [
        IssueLocator(CINTable.ChildIdentifiers, UPN, 2),
        IssueLocator(CINTable.ChildIdentifiers, UPN, 3),
    ]

    assert result.definition.code == "1560Q"
    assert result.definition.message == "Please check: Former UPN wrongly formatted"
