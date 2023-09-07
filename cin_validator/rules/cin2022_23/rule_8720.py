from typing import Mapping

import pandas as pd

from cin_validator.rule_engine import (
    CINTable,
    IssueLocator,
    RuleContext,
    rule_definition,
)
from cin_validator.test_engine import run_rule

# Get tables and columns of interest from the CINTable object defined in rule_engine/__api.py

ChildProtectionPlans = CINTable.ChildProtectionPlans
CPPstartDate = ChildProtectionPlans.CPPstartDate
Header = CINTable.Header
ReferenceDate = Header.ReferenceDate
CPPID = ChildProtectionPlans.CPPID


# define characteristics of rule
@rule_definition(
    # write the rule code here, in place of '8720'
    code="8720",
    # replace ChildIdentifiers with the value in the module column of the excel sheet corresponding to this rule .
    module=CINTable.ChildProtectionPlans,
    # replace the message with the corresponding value for this rule, gotten from the excel sheet.
    message="Child Protection Plan Start Date missing or out of data collection period",
    # The column names tend to be the words within the < > signs in the github issue description.
    affected_fields=[CPPstartDate, ReferenceDate],
)
def validate(
    data_container: Mapping[CINTable, pd.DataFrame], rule_context: RuleContext
):
    # Replace ChildIdentifiers with the name of the table you need.
    df = data_container[ChildProtectionPlans]

    # ReferenceDate exists in the heder table so we get header table too.
    df_ref = data_container[Header]

    # Where a CPP module is present, <CPPstartDate> (N00105) must be present and on or before the <ReferenceDate> (N00603)
    condition = (df[CPPID].notna()) & (
        (df[CPPstartDate].isna()) | (df[CPPstartDate] > df_ref[ReferenceDate].iloc[0])
    )

    failing_indices = df[condition].index

    # Replace ChildProtectionPlans and CPPstartDate with the table and column name concerned in your rule, respectively.
    # If there are multiple columns or table, make this sentence multiple times.
    rule_context.push_issue(
        table=ChildProtectionPlans, field=CPPstartDate, row=failing_indices
    )


def test_validate():
    # Create some sample data such that some values pass the validation and some fail.
    fake_header = pd.DataFrame(
        [{ReferenceDate: "31/03/2022"}]  # the census start date here will be 01/04/2021
    )

    fake_cpp = pd.DataFrame(
        [
            {
                CPPID: "ID1",
                CPPstartDate: "01/03/2019",
            },  # Pass: March 1st is before April 1st, 2021. It is out of range
            {CPPID: pd.NA, CPPstartDate: pd.NA},  # 1 pass: No CPPID
            {
                CPPID: "ID1",
                CPPstartDate: "01/10/2022",
            },  # 2 fail: October 1st is after March 31st, 2022. It is out of range
            {
                CPPID: "ID1",
                CPPstartDate: pd.NA,
            },  # 2 fail: October 1st is after March 31st, 2022. It is out of range
        ]
    )

    # if date columns are involved, the validate function will be expecting them as dates so convert before passing them in.
    fake_cpp[CPPstartDate] = pd.to_datetime(
        fake_cpp[CPPstartDate], format="%d/%m/%Y", errors="coerce"
    )
    fake_header[ReferenceDate] = pd.to_datetime(
        fake_header[ReferenceDate], format="%d/%m/%Y", errors="coerce"
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
        IssueLocator(CINTable.ChildProtectionPlans, CPPstartDate, 2),
        IssueLocator(CINTable.ChildProtectionPlans, CPPstartDate, 3),
    ]

    # Check that the rule definition is what you wrote in the context above.

    # replace '8720' with the rule code and put the appropriate message in its place too.
    assert result.definition.code == "8720"
    assert (
        result.definition.message
        == "Child Protection Plan Start Date missing or out of data collection period"
    )
