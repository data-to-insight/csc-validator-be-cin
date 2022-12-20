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
# Replace ChildIdentifiers with the table name, and LAChildID with the column name you want.

Cindetails = CINTable.CINdetails
CINreferralDate = Cindetails.CINreferralDate
Header = CINTable.Header
ReferenceDate = Header.ReferenceDate

# define characteristics of rule
@rule_definition(
    code=8600,
    module=CINTable.CINdetails,
    message="Child referral date missing or after data collection period",
    affected_fields=[CINreferralDate],
)
def validate(
    data_container: Mapping[CINTable, pd.DataFrame], rule_context: RuleContext
):

    # Replace ChildIdentifiers with the name of the table you need as well as a DataFrane for the Header as we are using referencing the Census Period EndDate
    df = data_container[Cindetails]
    df_ref = data_container[Header]

    # Select out only the ReferenceDate column from the DataFrame
    ref_data_series = df_ref[ReferenceDate]

    # Allocate to the start and end of the census period to collection_start and collection_end
    collection_start, collection_end = make_census_period(ref_data_series)

    # Implement rule logic as described by the Github issue.
    # Put the description as a comment above the implementation as shown.

    # Check there is a value recorded for the CINreferralDate
    df = df[df[CINreferralDate].notna()]

    # Where a CINreferralDate exists check to see if it is after the end of the Census Period (collection_end)
    df = df[(df[CINreferralDate] > collection_end)]

    failing_indices = df.index

    # Replace ChildIdentifiers and LAchildID with the table and column name concerned in your rule, respectively.
    # If there are multiple columns or table, make this sentence multiple times.
    rule_context.push_issue(
        table=Cindetails, field=CINreferralDate, row=failing_indices
    )


def test_validate():
    # Create some sample data such that some values pass the validation and some fail.

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

    # Run the rule function, passing in our sample data.
    result = run_rule(validate, {Cindetails: fake_refdates, Header: fake_header})

    # The result contains a list of issues encountered
    issues = list(result.issues)

    # replace 2 with the number of failing points you expect from the sample data.
    assert len(issues) == 2

    # replace the table and column name as done earlier.
    # The last numbers represent the index values where you expect the sample data to fail the validation check.
    assert issues == [
        IssueLocator(CINTable.CINdetails, CINreferralDate, 2),
        IssueLocator(CINTable.CINdetails, CINreferralDate, 4),
    ]

    # Check that the rule definition is what you wrote in the context above.

    # replace 2885 with the rule code and put the appropriate message in its place too.
    assert result.definition.code == 8600
    assert (
        result.definition.message
        == "Child referral date missing or after data collection period"
    )
