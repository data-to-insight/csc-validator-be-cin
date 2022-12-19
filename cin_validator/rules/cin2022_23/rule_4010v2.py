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

    # Allocate to the start and end of the census period to collection_start and collection_end. 
    # Collection_end to be referenced as reference_date to match CIN Census terminology
    ref_data_series = df_ref[ReferenceDate]
    collection_start, reference_date = make_census_period(ref_data_series)
       
    # implement rule logic as described by the Github issue. Put the description as a comment above the implementation as shown.
    # Where a <CINPlanDates> module is present, <CINPlanStartDate> (N00689) must be present and on or before the <ReferenceDate> (N00603)
    # condition states that there must be a value in CINPlanStartDate and that value must be after the reference_date. 
    # As such any NULL values for CINPlanStartDate wil be flagged with the error message

    df = df[(df[CINPlanStartDate].isna() | (df[CINPlanStartDate] > reference_date))]

    failing_indices = df.index

    rule_context.push_issue(
        table=CINplanDates, field=CINPlanStartDate, row=failing_indices
    )

def test_validate():

    cin_start = pd.to_datetime(
    # Create some sample data such that some values pass the validation and some fail.
        [
                "2022-04-25", #Fail - CINPlanStartDate is after the reference_date of 31st March 2022
                "2022-03-01", #Pass - CINPlanStartDate is before the reference_date of 31st March 2022
                "2022-12-25", #Fail - CINPlanStartDate is after the reference_date of 31st March 2022
                "2021-04-27", #Pass - CINPlanStartDate is before the reference_date of 31st March 2022
                "2021-11-21", #Pass - CINPlanStartDate is before the reference_date of 31st March 2022
                "2021-08-20", #Pass - CINPlanStartDate is before the reference_date of 31st March 2022 
                "2020-04-17", #Pass - CINPlanStartDate is before the reference_date of 31st March 2022
                "1999-01-30", #Pass - CINPlanStartDate is before the reference_date of 31st March 2022
                pd.NA, #Fail - CINPlanStartDate is blank and is therefore missing. 
        ],
        format="%Y/%m/%d",
        errors="coerce",
    )

    fake_cin_start = pd.DataFrame({CINPlanStartDate: cin_start})
    fake_header = pd.DataFrame([{ReferenceDate: "31/03/2022"}])  # the reference date will be 31/03/2022 
    
    # Run rule function passing in our sample data
    result = run_rule(validate, {CINplanDates: fake_cin_start, Header: fake_header})

    # The result contains a list of issues encountered
    issues = list(result.issues)

    # replace 2 with the number of failing points you expect from the sample data.
    assert len(issues) == 3
    # replace the table and column name as done earlier.
    # The last numbers represent the index values where you expect the sample data to fail the validation check.
    assert issues == [
        IssueLocator(CINTable.CINplanDates, CINPlanStartDate, 0),
        IssueLocator(CINTable.CINplanDates, CINPlanStartDate, 2),
        IssueLocator(CINTable.CINplanDates, CINPlanStartDate, 8),
     ]

    # Check that the rule definition is what you wrote in the context above.

    # replace 8500 with the rule code and put the appropriate message in its place too.
    assert result.definition.code == 4010
    assert (
        result.definition.message
        == "CIN Plan start date is missing or out of data collection period"
           )