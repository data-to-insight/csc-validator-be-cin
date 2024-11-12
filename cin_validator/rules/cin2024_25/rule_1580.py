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
    code="1580",
    module=CINTable.ChildIdentifiers,
    message="LA Child ID must not contain any non-alphanumeric characters",
    affected_fields=[LAchildID],
)
def validate(
    data_container: Mapping[CINTable, pd.DataFrame], rule_context: RuleContext
):
    df = data_container[ChildIdentifiers].copy()
    non_alphanumeric = df[df["LAchildID"].str.isalnum() == False]

    failing_indices = non_alphanumeric.index

    rule_context.push_issue(
        table=ChildIdentifiers, field=LAchildID, row=failing_indices
    )


def test_validate():
    # Create some sample data such that some values pass the validation and some fail.
    # Sidenote: a typical Header table will only have one row.
    sample_ci = pd.DataFrame(
        {
            "LAchildID": [
                "XXXXXXXXXXXXXXXXXXXX",
                "XXXXXXXXXXXXXXXXXXX",
                "XXXXXXXXXXXXXXXXXXXX;",  # non-alphanumeric characters, fail
            ]
        }
    )

    result = run_rule(validate, {ChildIdentifiers: sample_ci})

    issues = list(result.issues)
    # Intended fail points in data.
    assert len(issues) == 1
    # Intended failures of test data by index.
    assert issues == [
        IssueLocator(CINTable.ChildIdentifiers, LAchildID, 2),
    ]

    # Checks rule code and message are correct.
    assert result.definition.code == "1580"
    assert (
        result.definition.message
        == "LA Child ID must not contain any non-alphanumeric characters"
    )
