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

CINPlanDates = CINTable.CINplanDates
CINPlanEndDate = CINPlanDates.CINPlanEndDate
Header = CINTable.Header
ReferenceDate = Header.ReferenceDate


@rule_definition(
    code="4013",
    module=CINTable.CINplanDates,
    message="CIN Plan end date must fall within the census year",
    affected_fields=[CINPlanEndDate, ReferenceDate],
)
def validate(
    data_container: Mapping[CINTable, pd.DataFrame], rule_context: RuleContext
):
    df = data_container[CINPlanDates]

    df_ref = data_container[Header]
    ref_date_series = df_ref[ReferenceDate]
    collection_start, reference_date = make_census_period(ref_date_series)

    # If <CINPlanEndDate> (N00690) is present, then<CINPlanEndDate> (N00690) must fall within [Period_of_Census] inclusive
    # A value is out of range if it is before the start or after the end.
    failing_indices = df[
        (df[CINPlanEndDate] < collection_start) | (df[CINPlanEndDate] > reference_date)
    ].index

    rule_context.push_issue(
        table=CINPlanDates, field=CINPlanEndDate, row=failing_indices
    )


def test_validate():
    fake_header = pd.DataFrame(
        [{ReferenceDate: "31/03/2022"}]  # the census start date here will be 01/04/2021
    )
    fake_CINEndDate = pd.DataFrame(
        [
            {
                CINPlanEndDate: "01/03/2019"
            },  # 0 fail: March 1st is before April 1st, 2021. It is out of range
            {
                CINPlanEndDate: "01/04/2021"
            },  # 1 pass: April 1st is within April 1st, 2021 to March 31st, 2022.
            {
                CINPlanEndDate: "01/10/2022"
            },  # 2 fail: October 1st is after March 31st, 2022. It is out of range
        ]
    )

    fake_CINEndDate[CINPlanEndDate] = pd.to_datetime(
        fake_CINEndDate[CINPlanEndDate], format="%d/%m/%Y", errors="coerce"
    )

    result = run_rule(validate, {CINPlanDates: fake_CINEndDate, Header: fake_header})

    issues = list(result.issues)
    assert len(issues) == 2
    assert issues == [
        # from above, index positions 0 and 2 fail.
        IssueLocator(CINTable.CINplanDates, CINPlanEndDate, 0),
        IssueLocator(CINTable.CINplanDates, CINPlanEndDate, 2),
    ]

    assert result.definition.code == "4013"
    assert (
        result.definition.message
        == "CIN Plan end date must fall within the census year"
    )
