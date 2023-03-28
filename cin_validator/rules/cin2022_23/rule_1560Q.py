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
FormerUPN = ChildIdentifiers.FormerUPN


@rule_definition(
    code="1560Q",
    module=CINTable.ChildIdentifiers,
    rule_type=RuleType.QUERY,
    message="Please check and either amend or provide a reason: Former UPN wrongly formatted",
    affected_fields=[FormerUPN],
)
def validate(
    data_container: Mapping[CINTable, pd.DataFrame], rule_context: RuleContext
):
    df = data_container[ChildIdentifiers]

    # <FormerUPN> (N00002) where present should be in the correct format, as specified in the data table

    # Note, there are multiple types of former UPN, there are Temporary UPNs which end in a letter, and
    # those where a child is assigned a UPN but then another is identified for them having been used previously.
    # If this was only a check for temporary UPNs, it would check that the last character was a letter. However, it checks more generally.

    #  filter rows of df where the UPN column doesn't have Na/NaN values
    df = df.loc[df[FormerUPN].notna()]

    # Flag locations where FormerUPN is not 13 characters long
    check_length = df[FormerUPN].str.len() != 13
    # Flag locations where FormerUPN's last twelve characters do not form a full digit
    digit_within = ~df[FormerUPN].str[1:].str.isdigit()
    # Flag locations where FormerUPN's first character is not a letter
    check_edges = ~df[FormerUPN].str[0].str.isalpha()

    failing_indices = df[check_length | digit_within | check_edges].index

    rule_context.push_issue(
        table=ChildIdentifiers, field=FormerUPN, row=failing_indices
    )


def test_validate():
    child_identifiers = pd.DataFrame(
        [
            {FormerUPN: pd.NA},  # 0 ignore
            {FormerUPN: "X987654321231"},  # 1 pass
            {FormerUPN: "X0000y0000007"},  # 2 fail non-alphabet within
            {FormerUPN: "X98721238"},  # 3 wrong length
            {FormerUPN: "E000215119000"},
            {FormerUPN: "X987654321231"},  # 1 pass
            {FormerUPN: "E000215119000"},
        ]
    )

    result = run_rule(validate, {ChildIdentifiers: child_identifiers})

    issues = list(result.issues)
    assert len(issues) == 2
    assert issues == [
        IssueLocator(CINTable.ChildIdentifiers, FormerUPN, 2),
        IssueLocator(CINTable.ChildIdentifiers, FormerUPN, 3),
    ]

    assert result.definition.code == "1560Q"
    assert (
        result.definition.message
        == "Please check and either amend or provide a reason: Former UPN wrongly formatted"
    )
