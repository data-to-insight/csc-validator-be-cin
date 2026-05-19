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
InitialPPMeetingDate = PreProceedings.InitialPPMeetingDate
ReviewMeetingsCount = PreProceedings.ReviewMeetingsCount


@rule_definition(
    code="9015Q",
    module=CINTable.PreProceedings,
    message="Please check: if review meetings were held  following the initial pre-proceedings meeting, the total number held should be reported.",
    affected_fields=[ReviewMeetingsCount],
)
def validate(
    data_container: Mapping[CINTable, pd.DataFrame], rule_context: RuleContext
):
    # If   (N00830) is present, then   (N00831) should be reported.
    df = data_container[PreProceedings]

    df_issues = df[df[InitialPPMeetingDate].notna() & df[ReviewMeetingsCount].isna()]

    failing_indices = df_issues.index

    rule_context.push_issue(
        table=PreProceedings, field=ReviewMeetingsCount, row=failing_indices
    )


def test_validate():
    sample_pp = pd.DataFrame(
        {
            "InitialPPMeetingDate": [
                pd.NA,
                1,
                1,
                0,
                pd.NA,
                0,
            ],
            "ReviewMeetingsCount": [
                pd.NA,
                pd.NA,
                "01/04/2001",
                "02/04/2000",
                "11/04/2000",
                "11/04/2000",
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

    assert len(issues) == 1

    assert issues == [
        IssueLocator(CINTable.PreProceedings, ReviewMeetingsCount, 1),
    ]

    assert result.definition.code == "9015Q"
    assert (
        result.definition.message
        == "Please check: if review meetings were held  following the initial pre-proceedings meeting, the total number held should be reported."
    )
