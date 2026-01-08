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

Header = CINTable.Header
ReferenceDate = Header.ReferenceDate


@rule_definition(
    code="9008",
    module=CINTable.PreProceedings,
    message="The date of the first effective pre-proceedings meeting must be within the reporting year.",
    affected_fields=[InitialPPMeetingDate],
)
def validate(
    data_container: Mapping[CINTable, pd.DataFrame], rule_context: RuleContext
):
    # If   (N00830) is present, then it must fall within [Period_of_Census] inclusive.
    df = data_container[PreProceedings]

    df_ref = data_container[Header]
    ref_date_series = df_ref[ReferenceDate]
    collection_start, collection_end = make_census_period(ref_date_series)

    df_issues = df[
        (df[InitialPPMeetingDate] > collection_end)
        | (df[InitialPPMeetingDate] < collection_start)
    ]

    failing_indices = df_issues.index

    rule_context.push_issue(
        table=PreProceedings, field=InitialPPMeetingDate, row=failing_indices
    )


def test_validate():
    sample_header = pd.DataFrame(
        [{ReferenceDate: "31/03/2001"}]  # collection_start is 01/04/2000
    )

    sample_pp = pd.DataFrame(
        {"InitialPPMeetingDate": ["30/03/2000", "01/04/2000", "01/04/2001"]}
    )

    sample_pp["InitialPPMeetingDate"] = pd.to_datetime(
        sample_pp["InitialPPMeetingDate"], dayfirst=True, errors="coerce"
    )

    result = run_rule(
        validate,
        {
            Header: sample_header,
            PreProceedings: sample_pp,
        },
    )

    issues = list(result.issues)

    assert len(issues) == 2

    assert issues == [
        IssueLocator(CINTable.PreProceedings, InitialPPMeetingDate, 0),
        IssueLocator(CINTable.PreProceedings, InitialPPMeetingDate, 2),
    ]

    assert result.definition.code == "9008"
    assert (
        result.definition.message
        == "The date of the first effective pre-proceedings meeting must be within the reporting year."
    )
