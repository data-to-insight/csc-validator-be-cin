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

CINplanDates = CINTable.CINplanDates
LAchildID = CINplanDates.LAchildID
CINPlanStartDate = CINplanDates.CINPlanStartDate
Header = CINTable.Header
ReferenceDate = Header.ReferenceDate

# define characteristics of rule
@rule_definition(
    code=4010,
    module=CINTable.CINplanDates,
    message="CIN Plan start date is missing or out of data collection period",
    affected_fields=[CINPlanStartDate],
)
def validate(
    data_container: Mapping[CINTable, pd.DataFrame], rule_context: RuleContext
):
    # Replace ChildIdentifiers with the name of the table you need as well as a DataFrane for the Header as we are using referencing the Census Period EndDate
    df = data_container[CINplanDates]
    df_ref = data_container[Header]

    # Select out only the ReferenceDate column from the DataFrame
    ref_data_series = df_ref[ReferenceDate]

    # Allocate to the start and end of the census period to collection_start and collection_end
    collection_start, collection_end = make_census_period(ref_data_series)

    # implement rule logic as described by the Github issue. Put the description as a comment above the implementation as shown.
    # Where a <CINPlanDates> module is present, <CINPlanStartDate> (N00689) must be present and on or before the <ReferenceDate> (N00603)

    # Check there is a value recorded for the CINPlanStartDate
    df = df[df[CINPlanStartDate].notna()]

    # Where a CINPlanStartDate exists check to see if it is after the end of the Census Period (collection_end)
    df = df[(df[CINPlanStartDate] > collection_end)]

    failing_indices = df.index

    # Replace ChildIdentifiers and LAchildID with the table and column name concerned in your rule, respectively.
    # If there are multiple columns or table, make this sentence multiple times.
    rule_context.push_issue(
        table=CINplanDates, field=CINPlanStartDate, row=failing_indices
    )


def test_validate():
    # Create some sample data such that some values pass the validation and some fail.

    cin_start = pd.to_datetime(
        [
            "25/04/2022",  # fail (date is after the end of the census period end date of 31st March 2022)
            "01/03/2022",  # pass (date is before the end of the census period of 31st March 2022)
            "25/12/2022",  # fail (date is after the end of the census period end date of 31st March 2022)
            "27/04/2021",  # pass (date is before the end of the census period of 31st March 2022)
            "21/11/2021",  # pass (date is before the end of the census period of 31st March 2022)
            "20/08/2021",  # pass (date is before the end of the census period of 31st March 2022)
            "17/04/2020",  # pass (date is before the end of the census period of 31st March 2022)
            pd.NA,
        ],
        format="%d/%m/%Y",
        errors="coerce",
    )

    fake_cin_start = pd.DataFrame({CINPlanStartDate: cin_start})
    fake_header = pd.DataFrame(
        [{ReferenceDate: "31/03/2022"}]
    )  # the census start date here will be 01/04/2021

    # Run rule function passing in our sample data
    result = run_rule(validate, {CINplanDates: fake_cin_start, Header: fake_header})

    # The result contains a list of issues encountered
    issues = list(result.issues)

    # replace 2 with the number of failing points you expect from the sample data.
    assert len(issues) == 2
    # replace the table and column name as done earlier.
    # The last numbers represent the index values where you expect the sample data to fail the validation check.
    assert issues == [
        IssueLocator(CINTable.CINplanDates, CINPlanStartDate, 0),
        IssueLocator(CINTable.CINplanDates, CINPlanStartDate, 2),
    ]

    # Check that the rule definition is what you wrote in the context above.

    # replace 8500 with the rule code and put the appropriate message in its place too.
    assert result.definition.code == 4010
    assert (
        result.definition.message
        == "CIN Plan start date is missing or out of data collection period"
    )
