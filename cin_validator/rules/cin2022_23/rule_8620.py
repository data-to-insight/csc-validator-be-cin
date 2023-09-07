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

# Get tables and columns of interest from the CINTable object defined in rule_engine/__api.py

CINdetails = CINTable.CINdetails
CINclosureDate = CINdetails.CINclosureDate
Header = CINTable.Header
ReferenceDate = Header.ReferenceDate


# define characteristics of rule
@rule_definition(
    code="8620",
    module=CINTable.CINdetails,
    message="CIN Closure Date present and does not fall within the Census year",
    affected_fields=[CINclosureDate],
)
def validate(
    data_container: Mapping[CINTable, pd.DataFrame], rule_context: RuleContext
):
    df = data_container[CINdetails]
    df_ref = data_container[Header]

    # ReferenceDate exists in the header table so we get header table too.
    ref_date_series = df_ref[ReferenceDate]

    # the make_census_period function generates the start and end date so that you don't have to do it each time.
    collection_start, collection_end = make_census_period(ref_date_series)

    # implement rule logic as described by the Github issue. Put the description as a comment above the implementation as shown.

    # If <CINclosureDate> (N00102) is present, it must be within [Period_of_Census]
    df = df[df[CINclosureDate].notna()]
    df = df[
        ~(
            (df[CINclosureDate] >= collection_start)
            & (df[CINclosureDate] <= collection_end)
        )
    ]
    failing_indices = df.index

    rule_context.push_issue(table=CINdetails, field=CINclosureDate, row=failing_indices)


def test_validate():
    # Create some sample data such that some values pass the validation and some fail.
    fake_header = pd.DataFrame(
        {ReferenceDate: ["31/03/2022"]}  # the census start date here will be 01/04/2021
    )
    fake_cinclosure = pd.DataFrame(
        [
            {
                CINclosureDate: "01/03/2019"
            },  # 0 fail: March 1st is before April 1st, 2021. It is out of range
            {
                CINclosureDate: "10/04/2021"
            },  # 1 pass: April 10th is within April 1st, 2021 to March 31st, 2022.
            {
                CINclosureDate: "01/10/2022"
            },  # 2 fail: October 1st is after March 31st, 2022. It is out of range
            {CINclosureDate: "01/04/2021"},  # Pass, first day of census period
            {CINclosureDate: "31/03/2021"},  # Pass, last day of census period
        ]
    )

    # if date columns are involved, the validate function will be expecting them as dates so convert before passing them in.
    fake_cinclosure[CINclosureDate] = pd.to_datetime(
        fake_cinclosure[CINclosureDate], format="%d/%m/%Y", errors="coerce"
    )

    # Run rule function passing in our sample data
    result = run_rule(validate, {CINdetails: fake_cinclosure, Header: fake_header})

    # The result contains a list of issues encountered
    issues = list(result.issues)
    # replace 2 with the number of failing points you expect from the sample data.
    assert len(issues) == 2
    # replace the table and column name as done earlier.
    # The last numbers represent the index values where you expect the sample data to fail the validation check.
    assert issues == [
        IssueLocator(CINTable.CINdetails, CINclosureDate, 0),
        IssueLocator(CINTable.CINdetails, CINclosureDate, 2),
    ]

    # Check that the rule definition is what you wrote in the context above.

    # replace '8620' with the rule code and put the appropriate message in its place too.
    assert result.definition.code == "8620"
    assert (
        result.definition.message
        == "CIN Closure Date present and does not fall within the Census year"
    )
