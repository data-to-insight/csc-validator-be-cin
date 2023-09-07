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

Section47 = CINTable.Section47
DateOfInitialCPC = Section47.DateOfInitialCPC
Header = CINTable.Header
ReferenceDate = Header.ReferenceDate


# define characteristics of rule
@rule_definition(
    code="8715",
    module=CINTable.Section47,
    message="Date of Initial Child Protection Conference must fall within the census year",
    affected_fields=[DateOfInitialCPC, ReferenceDate],
)
def validate(
    data_container: Mapping[CINTable, pd.DataFrame], rule_context: RuleContext
):
    df = data_container[Section47]

    # ReferenceDate exists in the header table so we get header table too.
    df_ref = data_container[Header]
    ref_date_series = df_ref[ReferenceDate]
    # the make_census_period function generates the start and end date so that you don't have to do it each time.
    collection_start, reference_date = make_census_period(ref_date_series)

    # implement rule logic as described by the Github issue. Put the description as a comment above the implementation as shown.

    # If present <DateOfInitialCPC> (N00110) must be on or between [Start_Of_Census_Year] and <ReferenceDate> (N00603)
    # A value is out of range if it is before the start or after the end.
    failing_indices = df[
        (df[DateOfInitialCPC] < collection_start)
        | (df[DateOfInitialCPC] > reference_date)
    ].index

    # Replace Section47 and DateOfInitialCPC with the table and column name concerned in your rule, respectively.
    rule_context.push_issue(
        table=Section47, field=DateOfInitialCPC, row=failing_indices
    )


def test_validate():
    # Create some sample data such that some values pass the validation and some fail.
    fake_header = pd.DataFrame(
        [{ReferenceDate: "31/03/2022"}]  # the census start date here will be 01/04/2021
    )
    fake_section47 = pd.DataFrame(
        [
            {
                DateOfInitialCPC: "01/03/2019"
            },  # 0 fail: March 1st is before April 1st, 2021. It is out of range
            {
                DateOfInitialCPC: "01/04/2021"
            },  # 1 pass: April 1st is within April 1st, 2021 to March 31st, 2022.
            {
                DateOfInitialCPC: "01/10/2022"
            },  # 2 fail: October 1st is after March 31st, 2022. It is out of range
        ]
    )

    # if date columns are involved, the validate function will be expecting them as dates so convert before passing them in.
    fake_section47[DateOfInitialCPC] = pd.to_datetime(
        fake_section47[DateOfInitialCPC], format="%d/%m/%Y", errors="coerce"
    )

    # Run rule function passing in our sample data
    # Since the ReferenceDate comes from the Header column, we provide that also.
    result = run_rule(validate, {Section47: fake_section47, Header: fake_header})

    # The result contains a list of issues encountered
    issues = list(result.issues)
    # replace 2 with the number of failing points you expect from the sample data.
    assert len(issues) == 2
    # replace the table and column name as done earlier.
    # The last numbers represent the index values where you expect the sample data to fail the validation check.
    assert issues == [
        # from above, index positions 0 and 2 fail.
        IssueLocator(CINTable.Section47, DateOfInitialCPC, 0),
        IssueLocator(CINTable.Section47, DateOfInitialCPC, 2),
    ]

    # Check that the rule definition is what you wrote in the context above.

    # replace '8715' with the rule code and put the appropriate message in its place too.
    assert result.definition.code == "8715"
    assert (
        result.definition.message
        == "Date of Initial Child Protection Conference must fall within the census year"
    )
