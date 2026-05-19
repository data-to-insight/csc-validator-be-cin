from typing import Mapping

import pandas as pd

from cin_validator.rule_engine import (
    CINTable,
    IssueLocator,
    RuleContext,
    rule_definition,
)
from cin_validator.test_engine import run_rule
from cin_validator.utils import make_census_period

PreProceedings = CINTable.PreProceedings
PPOutcome = PreProceedings.PPOutcome


@rule_definition(
    code="9011",
    module=CINTable.PreProceedings,
    message="The outcome of the pre-proceedings must be reported using a valid value",
    affected_fields=[PPOutcome],
)
def validate(
    data_container: Mapping[CINTable, pd.DataFrame], rule_context: RuleContext
):
    # If (N00833) is present, then it must be a valid value (codeset CS131).
    df = data_container[PreProceedings]

    df_issues = df[(~df[PPOutcome].isin(["A", "B", "C"]))]

    failing_indices = df_issues.index

    rule_context.push_issue(table=PreProceedings, field=PPOutcome, row=failing_indices)


def test_validate():
    sample_pp = pd.DataFrame(
        {
            "PPOutcome": [
                "A",
                "B",
                "C",
                "D",
                pd.NA,
                0,
            ],
        }  # 1 fails
    )

    result = run_rule(
        validate,
        {
            PreProceedings: sample_pp,
        },
    )

    issues = list(result.issues)

    assert len(issues) == 3

    assert issues == [
        IssueLocator(CINTable.PreProceedings, PPOutcome, 3),
        IssueLocator(CINTable.PreProceedings, PPOutcome, 4),
        IssueLocator(CINTable.PreProceedings, PPOutcome, 5),
    ]

    assert result.definition.code == "9011"
    assert (
        result.definition.message
        == "The outcome of the pre-proceedings must be reported using a valid value"
    )
