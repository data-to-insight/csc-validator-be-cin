"""
Rule number: '8510'
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

ChildIdentifiers = CINTable.ChildIdentifiers
LAchildID = ChildIdentifiers.LAchildID


@rule_definition(
    code="8510",
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
    child_identifiers = pd.DataFrame([[1234], [1234], [346546]], columns=[LAchildID])

    result = run_rule(validate, {ChildIdentifiers: child_identifiers})

    issues = list(result.issues)
    assert len(issues) == 2
    assert issues == [
        IssueLocator(CINTable.ChildIdentifiers, LAchildID, 0),
        IssueLocator(CINTable.ChildIdentifiers, LAchildID, 1),
    ]

    assert result.definition.code == "8510"
    assert (
        result.definition.message
        == "More than one child record with the same LA Child ID"
    )
