"""
Rule number: 8510
Module: Child idenitifiers
Rule details: Each <LAchildID> (N00097) must be unique across all children within the same LA return. 

Note: This rule should be evaluated at LA-level for imported data

Rule message: More than one child record with the same LA Child ID

"""
from typing import Mapping

import pandas as pd

from cin_validator.rule_engine import (
    CINTable,
    IssueLocator,
    RuleContext,
    rule_definition,
)
from cin_validator.test_engine import run_rule

# Get tables and columns of interest from the CINTable object defined in rule_engine/__api.py

ChildIdentifiers = CINTable.ChildIdentifiers
LAchildID = ChildIdentifiers.LAchildID

# define characteristics of rule
@rule_definition(
    code=8510,
    module=CINTable.ChildIdentifiers,
    message="More than one child record with the same LA Child ID",
    affected_fields=[LAchildID],
)
def validate(
    data_container: Mapping[CINTable, pd.DataFrame], rule_context: RuleContext
):
    df = data_container[ChildIdentifiers]

    # Each <LAchildID> (N00097) must be unique across all children within the same LA return

    failing_indices = df[df.duplicated(subset=[LAchildID], keep=False)].index

    rule_context.push_issue(
        table=ChildIdentifiers, field=LAchildID, row=failing_indices
    )


def test_validate():
    # Create some sample data such that some values pass the validation and some fail.
    child_identifiers = pd.DataFrame([[1234], [1234], [346546]], columns=[LAchildID])

    # Run rule function passing in our sample data
    result = run_rule(validate, {ChildIdentifiers: child_identifiers})

    # The result contains a list of issues encountered
    issues = list(result.issues)
    # replace 2 with the number of failing points you expect from the sample data.
    assert len(issues) == 2
    # replace the table and column name as done earlier.
    # The last numbers represent the index values where you expect the sample data to fail the validation check.
    assert issues == [
        IssueLocator(CINTable.ChildIdentifiers, LAchildID, 0),
        IssueLocator(CINTable.ChildIdentifiers, LAchildID, 1),
    ]

    # Check that the rule definition is what you wrote in the context above.

    # replace 8500 with the rule code and put the appropriate message in its place too.
    assert result.definition.code == 8510
    assert (
        result.definition.message
        == "More than one child record with the same LA Child ID"
    )
