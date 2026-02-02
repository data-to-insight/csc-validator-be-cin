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
LBPSentDate = PreProceedings.LBPSentDate

Header = CINTable.Header
ReferenceDate = Header.ReferenceDate


@rule_definition(
    code="9006",
    module=CINTable.PreProceedings,
    message="The date the letter before proceedings was sent must be within the reporting year and after the date pre-proceedings began.",
    affected_fields=[LBPSentDate],
)
def validate(
    data_container: Mapping[CINTable, pd.DataFrame], rule_context: RuleContext
):
    # If (N00827)  is present, then (N00827)  must fall within [Period_of_Census] inclusive and on or after (N00826).
    df = data_container[PreProceedings]

    # Must ignore empty PPStartDates as this will cause an error when comparing dates if incorrectly input
    df = df[df[LBPSentDate].notna() & df[PPStartDate].notna()]

    df_ref = data_container[Header]
    ref_date_series = df_ref[ReferenceDate]
    collection_start, collection_end = make_census_period(ref_date_series)

    df_issues = df[
        (df[LBPSentDate] > collection_end)
        | (df[LBPSentDate] < collection_start)
        | (df[LBPSentDate] < df[PPStartDate])
    ]

    failing_indices = df_issues.index

    rule_context.push_issue(
        table=PreProceedings, field=LBPSentDate, row=failing_indices
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
            "LBPSentDate": [
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

    sample_pp["LBPSentDate"] = pd.to_datetime(
        sample_pp["LBPSentDate"], dayfirst=True, errors="coerce"
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
        IssueLocator(CINTable.PreProceedings, LBPSentDate, 1),
        IssueLocator(CINTable.PreProceedings, LBPSentDate, 2),
        IssueLocator(CINTable.PreProceedings, LBPSentDate, 3),
    ]

    assert result.definition.code == "9006"
    assert (
        result.definition.message
        == "The date the letter before proceedings was sent must be within the reporting year and after the date pre-proceedings began."
    )
