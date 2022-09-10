from typing import Mapping

import pandas as pd

from cin_validator.rule_engine import rule_definition, Module, RuleContext
from cin_validator.rule_engine import IssueLocator
from cin_validator.test_engine import run_rule
from cin_validator.type_definitions import CINTables


@rule_definition(
    code=8500,
    module=Module.CHILD_IDENTIFIERS,
    description="LA Child ID missing",
    affected_fields=["LAchildID"],
)
def validate_8500(
    data_container: Mapping[CINTables, pd.DataFrame], rule_context: RuleContext
):
    df = data_container[CINTables.CHILD_IDENTIFIERS]

    # select all the locations where the child ID is absent
    failing_indices = df[df["LAchildID"].isna()].index

    # We can push the entire index to keep code small and memory usage low
    rule_context.push_issue(
        table=CINTables.CHILD_IDENTIFIERS, field="LAchildID", row=failing_indices
    )


def test_validate_8500():
    # Create some sample data
    child_identifiers = pd.DataFrame([[1234], [pd.NA], [pd.NA]], columns=["LAchildID"])

    # Run rule function passing in our sample data
    result = run_rule(validate_8500, {CINTables.CHILD_IDENTIFIERS: child_identifiers})

    # The result contains a list of issues encountered
    issues = list(result.issues)
    assert len(issues) == 2
    assert issues == [
        IssueLocator(CINTables.CHILD_IDENTIFIERS, "LAchildID", 1),
        IssueLocator(CINTables.CHILD_IDENTIFIERS, "LAchildID", 2),
    ]

    # As well as the rule definition
    assert result.definition.code == 8500
    assert result.definition.description == "LA Child ID missing"
