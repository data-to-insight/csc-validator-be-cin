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
PPStartDate = PreProceedings.PPStartDate
StepDecisionDate = PreProceedings.StepDecisionDate

Header = CINTable.Header
ReferenceDate = Header.ReferenceDate


@rule_definition(
    code="9009",
    module=CINTable.PreProceedings,
    message="The date when the decision was made to step up or down must be on or after the date of the decision to begin pre-proceedings and must fall within the reporting year",
    affected_fields=[StepDecisionDate],
)
def validate(
    data_container: Mapping[CINTable, pd.DataFrame], rule_context: RuleContext
):
    # If (N00827)  is present, then (N00827)  must fall within [Period_of_Census] inclusive and on or after (N00826).
    df = data_container[PreProceedings]

    # Must ignore empty PPStartDates as this will cause an error when comparing dates if incorrectly input
    df = df[df[StepDecisionDate].notna() & df[PPStartDate].notna()]

    df_ref = data_container[Header]
    ref_date_series = df_ref[ReferenceDate]
    collection_start, collection_end = make_census_period(ref_date_series)

    df_issues = df[
        (df[StepDecisionDate] > collection_end)
        | (df[StepDecisionDate] < collection_start)
        | (df[StepDecisionDate] < df[PPStartDate])
    ]

    failing_indices = df_issues.index

    rule_context.push_issue(
        table=PreProceedings, field=StepDecisionDate, row=failing_indices
    )


def test_validate():
    sample_header = pd.DataFrame(
        [{ReferenceDate: "31/03/2001"}]  # collection_start is 01/04/2000
    )

    sample_pp = pd.DataFrame(
        {
            "PPStartDate": [
                "10/04/2000",
                "10/04/2000",
                "10/04/2000",
                "10/04/2000",
                pd.NA,
                "10/04/200",
            ],
            "StepDecisionDate": [
                pd.NA,
                "30/03/2000",
                "01/04/2001",
                "02/04/2000",
                "11/04/2000",
                "11/04/2000",
            ],
        }  # 0 pass no data, 1 fail before census, 2 fail after census, 3 fail before PPStartDate, 4 pass no PPStartDate, 5 pass correct
    )

    sample_pp["PPStartDate"] = pd.to_datetime(
        sample_pp["PPStartDate"], dayfirst=True, errors="coerce"
    )

    sample_pp["StepDecisionDate"] = pd.to_datetime(
        sample_pp["StepDecisionDate"], dayfirst=True, errors="coerce"
    )

    result = run_rule(
        validate,
        {
            Header: sample_header,
            PreProceedings: sample_pp,
        },
    )

    issues = list(result.issues)

    assert len(issues) == 3

    assert issues == [
        IssueLocator(CINTable.PreProceedings, StepDecisionDate, 1),
        IssueLocator(CINTable.PreProceedings, StepDecisionDate, 2),
        IssueLocator(CINTable.PreProceedings, StepDecisionDate, 3),
    ]

    assert result.definition.code == "9009"
    assert (
        result.definition.message
        == "The date when the decision was made to step up or down must be on or after the date of the decision to begin pre-proceedings and must fall within the reporting year"
    )
