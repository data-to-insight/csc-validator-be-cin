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
StepDecisionDate = PreProceedings.StepDecisionDate
CourtAppDate = PreProceedings.CourtAppDate

Header = CINTable.Header
ReferenceDate = Header.ReferenceDate


@rule_definition(
    code="9012",
    module=CINTable.PreProceedings,
    message="The date that the local authority submitted the court application to commence care proceedings must fall within the reporting year and be on or after the date of the decision to initiate care proceedings.",
    affected_fields=[CourtAppDate],
)
def validate(
    data_container: Mapping[CINTable, pd.DataFrame], rule_context: RuleContext
):
    # If (N00834) is present, then it must fall on or after (N008342 and must fall  within [Period_of_Census] inclusive.
    df = data_container[PreProceedings]

    # Must ignore empty PPStartDates as this will cause an error when comparing dates if incorrectly input
    df = df[df[CourtAppDate].notna() & df[StepDecisionDate].notna()]

    df_ref = data_container[Header]
    ref_date_series = df_ref[ReferenceDate]
    collection_start, collection_end = make_census_period(ref_date_series)

    df_issues = df[
        (df[CourtAppDate] > collection_end)
        | (df[CourtAppDate] < collection_start)
        | (df[CourtAppDate] < df[StepDecisionDate])
    ]

    failing_indices = df_issues.index

    rule_context.push_issue(
        table=PreProceedings, field=CourtAppDate, row=failing_indices
    )


def test_validate():
    sample_header = pd.DataFrame(
        [{ReferenceDate: "31/03/2001"}]  # collection_start is 01/04/2000
    )

    sample_pp = pd.DataFrame(
        {
            "StepDecisionDate": [
                "10/04/2000",
                "10/04/2000",
                "10/04/2000",
                "10/04/2000",
                pd.NA,
                "10/04/200",
            ],
            "CourtAppDate": [
                pd.NA,
                "30/03/2000",
                "01/04/2001",
                "02/04/2000",
                "11/04/2000",
                "11/04/2000",
            ],
        }  # 0 pass no data, 1 fail before census, 2 fail after census, 3 fail before PPStartDate, 4 pass no PPStartDate, 5 pass correct
    )

    sample_pp["CourtAppDate"] = pd.to_datetime(
        sample_pp["CourtAppDate"], dayfirst=True, errors="coerce"
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
        IssueLocator(CINTable.PreProceedings, CourtAppDate, 1),
        IssueLocator(CINTable.PreProceedings, CourtAppDate, 2),
        IssueLocator(CINTable.PreProceedings, CourtAppDate, 3),
    ]

    assert result.definition.code == "9012"
    assert (
        result.definition.message
        == "The date that the local authority submitted the court application to commence care proceedings must fall within the reporting year and be on or after the date of the decision to initiate care proceedings."
    )
