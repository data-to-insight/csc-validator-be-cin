from typing import Mapping

import pandas as pd

from cin_validator.rule_engine import rule_definition, CINTable, RuleContext
from cin_validator.rule_engine import IssueLocator
from cin_validator.test_engine import run_rule

ChildIdentifiers = CINTable.ChildIdentifiers
LAchildID = ChildIdentifiers.LAchildID


@rule_definition(
    code=8500,
    module=CINTable.ChildIdentifiers,
    message="LA Child ID missing",
    affected_fields=[LAchildID],
)
def validate(
    data_container: Mapping[CINTable, pd.DataFrame], rule_context: RuleContext
):

    df = data_container[ChildIdentifiers]

    # select all the locations where the child ID is absent
    failing_indices = df[df[LAchildID].isna()].index

    # We can push the entire index to keep code small and memory usage low
    rule_context.push_issue(
        table=ChildIdentifiers, field=LAchildID, row=failing_indices
    )


def test_validate():
    # Create some sample data
    child_identifiers = pd.DataFrame([[1234], [pd.NA], [pd.NA]], columns=[LAchildID])

    # Run rule function passing in our sample data
    result = run_rule(validate, {ChildIdentifiers: child_identifiers})

    # The result contains a list of issues encountered
    issues = list(result.issues)
    assert len(issues) == 2
    assert issues == [
        IssueLocator(CINTable.ChildIdentifiers, LAchildID, 1),
        IssueLocator(CINTable.ChildIdentifiers, LAchildID, 2),
    ]

    # As well as the rule definition
    assert result.definition.code == 8500
    assert result.definition.description == "LA Child ID missing"
