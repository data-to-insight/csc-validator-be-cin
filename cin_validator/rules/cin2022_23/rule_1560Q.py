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

# Get tables and columns of interest from the CINTable object defined in rule_engine/__api.py
# Replace ChildIdentifiers with the table name, and LAChildID with the column name you want.

ChildIdentifiers = CINTable.ChildIdentifiers
UPN = ChildIdentifiers.UPN

# define characteristics of rule
@rule_definition(
    # write the rule code here, in place of 8500
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

    # LOGIC
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

    # Replace ChildIdentifiers and LAchildID with the table and column name concerned in your rule, respectively.
    rule_context.push_issue(table=ChildIdentifiers, field=UPN, row=failing_indices)


def test_validate():
    # Create some sample data such that some values pass the validation and some fail.
    child_identifiers = pd.DataFrame(
        [
            {"UPN": pd.NA},  # 0 ignore
            {"UPN": "X98765432123B"},  # 1 pass
            {"UPN": "X0000y000000K"},  # 2 fail non-alphabet within
            {"UPN": "X9872123B"},  # 3 wrong length
        ]
    )

    # Run rule function passing in our sample data
    result = run_rule(validate, {ChildIdentifiers: child_identifiers})

    # The result contains a list of issues encountered
    issues = list(result.issues)
    # replace 2 with the number of failing points you expect from the sample data.
    assert len(issues) == 2
    # replace the table and column name as done earlier.
    # The last numbers represent the index values where you expect the sample data to fail the validation check.
    assert issues == [
        IssueLocator(CINTable.ChildIdentifiers, UPN, 2),
        IssueLocator(CINTable.ChildIdentifiers, UPN, 3),
    ]

    # Check that the rule definition is what you wrote in the context above.

    # replace "1560Q" with the rule code and put the appropriate message in its place too.
    assert result.definition.code == "1560Q"
    assert result.definition.message == "Please check: Former UPN wrongly formatted"
