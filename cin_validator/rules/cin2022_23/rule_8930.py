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

ChildProtectionPlans = CINTable.ChildProtectionPlans
CPPendDate = ChildProtectionPlans.CPPendDate
Header = CINTable.Header
ReferenceDate = Header.ReferenceDate


# define characteristics of rule
@rule_definition(
    # write the rule code here, in place of '8930'
    code="8930",
    # replace ChildProtectionPlans with the value in the module column of the excel sheet corresponding to this rule .
    module=CINTable.ChildProtectionPlans,
    # replace the message with the corresponding value for this rule, gotten from the excel sheet.
    message="Child Protection Plan End Date must fall within the census year",
    # The column names tend to be the words within the < > signs in the github issue description.
    affected_fields=[CPPendDate, ReferenceDate],
)
def validate(
    data_container: Mapping[CINTable, pd.DataFrame], rule_context: RuleContext
):
    df = data_container[ChildProtectionPlans]

    # ReferenceDate exists in the heder table so we get header table too.
    df_ref = data_container[Header]
    ref_date_series = df_ref[ReferenceDate]

    # the make_census_period function generates the start and end date so that you don't have to do it each time.
    collection_start, reference_date = make_census_period(ref_date_series)

    # implement rule logic as described by the Github issue. Put the description as a comment above the implementation as shown.

    # If <CPPendDate> (N00115) is present, then<CPPendDate> (N00115) must fall within [Period_of_Census] inclusive
    failing_indices = df[
        (df[CPPendDate] < collection_start) | (df[CPPendDate] > reference_date)
    ].index

    rule_context.push_issue(
        table=ChildProtectionPlans, field=CPPendDate, row=failing_indices
    )


def test_validate():
    # Create some sample data such that some values pass the validation and some fail.
    fake_header = pd.DataFrame(
        [{ReferenceDate: "31/03/2022"}]  # the census start date here will be 01/04/2021
    )

    fake_cpp = pd.DataFrame(
        [
            {
                CPPendDate: "01/03/2019"
            },  # 0 fail: March 1st is before April 1st, 2021. It is out of range
            {
                CPPendDate: "01/04/2021"
            },  # 1 pass: April 1st is within April 1st, 2021 to March 31st, 2022.
            {
                CPPendDate: "01/10/2022"
            },  # 2 fail: October 1st is after March 31st, 2022. It is out of range
            {
                CPPendDate: pd.NA
            },  # 2 fail: October 1st is after March 31st, 2022. It is out of range
        ]
    )

    # if date columns are involved, the validate function will be expecting them as dates so convert before passing them in.
    fake_cpp[CPPendDate] = pd.to_datetime(
        fake_cpp[CPPendDate], format="%d/%m/%Y", errors="coerce"
    )

    # Run rule function passing in our sample data
    # Since the ReferenceDate comes from the Header column, we provide that also.
    result = run_rule(validate, {ChildProtectionPlans: fake_cpp, Header: fake_header})

    # The result contains a list of issues encountered
    issues = list(result.issues)
    # replace 2 with the number of failing points you expect from the sample data.
    assert len(issues) == 2
    # replace the table and column name as done earlier.
    # The last numbers represent the index values where you expect the sample data to fail the validation check.
    assert issues == [
        IssueLocator(CINTable.ChildProtectionPlans, CPPendDate, 0),
        IssueLocator(CINTable.ChildProtectionPlans, CPPendDate, 2),
    ]

    # Check that the rule definition is what you wrote in the context above.

    # replace '8930' with the rule code and put the appropriate message in its place too.
    assert result.definition.code == "8930"
    assert (
        result.definition.message
        == "Child Protection Plan End Date must fall within the census year"
    )
