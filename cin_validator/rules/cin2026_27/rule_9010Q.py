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
FGDMMeetingFac = PreProceedings.FGDMMeetingFac


@rule_definition(
    code="9010Q",
    module=CINTable.PreProceedings,
    message="Please check: You have reported that a FGDM meeting was offered in the letter before proceedings. Please confirm whether or not a FGDM meeting was facilitated in the pre-proceedings period following this offer.",
    affected_fields=[FGDMMeetingFac],
)
def validate(
    data_container: Mapping[CINTable, pd.DataFrame], rule_context: RuleContext
):
    # If (N00828) is present and equals true, then   (N00829) should be present.
    df = data_container[PreProceedings]

    df_issues = df[
        (df[FGDMMeetingOffer].astype("str") == "1") & (df[FGDMMeetingFac].isna())
    ]

    failing_indices = df_issues.index

    rule_context.push_issue(
        table=PreProceedings, field=FGDMMeetingFac, row=failing_indices
    )


def test_validate():
    sample_pp = pd.DataFrame(
        {
            "FGDMMeetingOffer": [
                pd.NA,
                1,
                1,
                0,
                pd.NA,
                0,
            ],
            "FGDMMeetingFac": [
                pd.NA,
                pd.NA,
                1,
                1,
                1,
                1,
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
        IssueLocator(CINTable.PreProceedings, FGDMMeetingFac, 1),
    ]

    assert result.definition.code == "9010Q"
    assert (
        result.definition.message
        == "Please check: You have reported that a FGDM meeting was offered in the letter before proceedings. Please confirm whether or not a FGDM meeting was facilitated in the pre-proceedings period following this offer."
    )
