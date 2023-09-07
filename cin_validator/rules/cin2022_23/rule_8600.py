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

Cindetails = CINTable.CINdetails
CINreferralDate = Cindetails.CINreferralDate
Header = CINTable.Header
ReferenceDate = Header.ReferenceDate


@rule_definition(
    code="8600",
    module=CINTable.CINdetails,
    message="Child referral date missing or after data collection period",
    affected_fields=[CINreferralDate],
)
def validate(
    data_container: Mapping[CINTable, pd.DataFrame], rule_context: RuleContext
):
    df = data_container[Cindetails]
    df_ref = data_container[Header]

    ref_data_series = df_ref[ReferenceDate]

    collection_start, collection_end = make_census_period(ref_data_series)

    # <CINreferralDate> (N00100) must be present and must be on or before <ReferenceDate> (N00603)
    condition = (df[CINreferralDate] > collection_end) | (df[CINreferralDate].isna())
    # Where a CINreferralDate exists check to see if it is after the end of the Census Period (collection_end)
    df = df[condition]

    failing_indices = df.index

    rule_context.push_issue(
        table=Cindetails, field=CINreferralDate, row=failing_indices
    )


def test_validate():
    sample_refdates = pd.to_datetime(
        [
            "2021-06-30",  # Pass - the date is before the collection_end of 31st March 2022
            "2022-01-25",  # Pass - the date is before the collection_end of 31st March 2022
            "2022-12-25",  # Fail - the date is after the collection_end of 31st March 2022
            pd.NA,  # Fail - the date field is blank and is therefore missing
            "2022-12-03",  # Fail - the date is after the collection_end of 31st March 2022
            "2021-08-20",  # Pass - the date is before the collection_end of 31st March 2022
            "2021-04-17",  # Pass - the date is before the collection_end of 31st March 2022
            "2001-01-25",  # Pass - the date is before the collection_end of 31st March 2022
            pd.NA,  # Fail - the date field is blank and is therefore missing
        ],
        format="%Y/%m/%d",
        errors="coerce",
    )

    fake_refdates = pd.DataFrame({CINreferralDate: sample_refdates})
    fake_header = pd.DataFrame(
        [{ReferenceDate: "31/03/2022"}]
    )  # the census start date here will be 01/04/2021

    result = run_rule(validate, {Cindetails: fake_refdates, Header: fake_header})

    issues = list(result.issues)

    assert len(issues) == 4

    assert issues == [
        IssueLocator(CINTable.CINdetails, CINreferralDate, 2),
        IssueLocator(CINTable.CINdetails, CINreferralDate, 3),
        IssueLocator(CINTable.CINdetails, CINreferralDate, 4),
        IssueLocator(CINTable.CINdetails, CINreferralDate, 8),
    ]

    assert result.definition.code == "8600"
    assert (
        result.definition.message
        == "Child referral date missing or after data collection period"
    )
