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
FGDMMeetingOffer = PreProceedings.FGDMMeetingOffer
LBPSentDate = PreProceedings.LBPSentDate


@rule_definition(
    code="9007",
    module=CINTable.PreProceedings,
    message="You have confirmed that a letter before proceedings was sent. Please confirm whether or not a FGDM meeting was offered in the letter.",
    affected_fields=[LBPSentDate],
)
def validate(
    data_container: Mapping[CINTable, pd.DataFrame], rule_context: RuleContext
):
    # If (N00827) is present, then (N00828) should be present.
    df = data_container[PreProceedings]

    df_issues = df[df[FGDMMeetingOffer].notna() & df[LBPSentDate].isna()]

    failing_indices = df_issues.index

    rule_context.push_issue(
        table=PreProceedings, field=LBPSentDate, row=failing_indices
    )


def test_validate():
    sample_pp = pd.DataFrame(
        {
            "FGDMMeetingOffer": [
                pd.NA,
                "10/04/2000",
                "10/04/2000",
                "10/04/2000",
                pd.NA,
                "10/04/200",
            ],
            "LBPSentDate": [
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
        IssueLocator(CINTable.PreProceedings, LBPSentDate, 1),
    ]

    assert result.definition.code == "9007"
    assert (
        result.definition.message
        == "You have confirmed that a letter before proceedings was sent. Please confirm whether or not a FGDM meeting was offered in the letter."
    )
