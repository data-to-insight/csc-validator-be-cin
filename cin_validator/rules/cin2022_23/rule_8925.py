from typing import Mapping
from cin_validator.rules.cin2022_23.rule_8500 import LAchildID

import pandas as pd

from cin_validator.rule_engine import rule_definition, CINTable, RuleContext
from cin_validator.rule_engine import IssueLocator
from cin_validator.test_engine import run_rule

# Get tables and columns of interest from the CINTable object defined in rule_engine/__api.py
# Replace ChildIdentifiers with the table name, and LAChildID with the column name you want.

ChildProtectionPlans = CINTable.ChildProtectionPlans
CPPendDate = ChildProtectionPlans.CPPendDate
CPPstartDate = ChildProtectionPlans.CPPstartDate
LAchildID = ChildProtectionPlans.LAchildID

# define characteristics of rule
@rule_definition(
    # write the rule code here, in place of 8500
    code=8925,
    # replace ChildIdentifiers with the value in the module column of the excel sheet corresponding to this rule .
    module=CINTable.ChildProtectionPlans,
    # replace the message with the corresponding value for this rule, gotten from the excel sheet.
    message="Child Protection Plan End Date earlier than Start Date",
    # The column names tend to be the words within the < > signs in the github issue description.
    affected_fields=[CPPstartDate, CPPendDate],
)
def validate(
    data_container: Mapping[CINTable, pd.DataFrame], rule_context: RuleContext
):
    # Replace ChildIdentifiers with the name of the table you need.
    df = data_container[ChildProtectionPlans]

    # implement rule logic as descriped by the Github issue. Put the description as a comment above the implementation as shown.

    # If present <CPPendDate> (N00115) must be on or after the <CPPstartDate> (N00105)
    df[CPPstartDate] = pd.to_datetime(df[CPPstartDate], format="%d/%m/%Y")
    df[CPPendDate] = pd.to_datetime(df[CPPendDate], format="%d/%m/%Y")
    df.reset_index(inplace=True)
    # keep the original index uncorrupted and select out only the rows where a comparison is possible.
    df_present = df[df[CPPstartDate].notna() & df[CPPendDate].notna()]
    df_issues = df_present[df_present[CPPendDate] < df_present[CPPstartDate]].set_index("index")
    start_error_locs = df_issues.loc[df_issues[CPPstartDate]==True].index
    idc = df_issues[LAchildID]
    end_error_locs = df_issues.loc[df_issues[CPPendDate]==True].index
    # Replace ChildIdentifiers and LAchildID with the table and column name concerned in your rule, respectively. 
    # If there are multiple columns or table, make this sentence multiple times.
    rule_context.push_linked_issues([(ChildProtectionPlans, CPPstartDate, start_error_locs, idc),(ChildProtectionPlans, CPPendDate, end_error_locs, idc),])


def test_validate():
    # Create some sample data such that some values pass the validation and some fail.
    child_protection_plans = pd.DataFrame([
        {"LAchildID": 1, "CPPstartDate": "26/05/2000" , "CPPendDate": "26/05/2000" ,},
        {"LAchildID": 2, "CPPstartDate": "26/05/2000" , "CPPendDate": "26/05/2001" ,},
        {"LAchildID": 3, "CPPstartDate": "26/05/2000" , "CPPendDate":  "26/05/1999",}, #2 error: end is before start
        {"LAchildID": 3, "CPPstartDate": "26/05/2000" , "CPPendDate": pd.NA,},
        {"LAchildID": 4, "CPPstartDate":  "26/05/2000" , "CPPendDate":  "25/05/2000",}, #4 error: end is before start
        {"LAchildID": 5, "CPPstartDate": pd.NA, "CPPendDate": pd.NA,},
    ])

    # Run rule function passing in our sample data
    result = run_rule(validate, {ChildProtectionPlans: child_protection_plans})

    # The result contains a list of issues encountered
    issues = list(result.linked_issues)
    print(issues)
    # replace 2 with the number of failing points you expect from the sample data.
    assert len(issues) == 2
    # replace the table and column name as done earlier. 
    # The last numbers represent the index values where you expect the sample data to fail the validation check.
    assert issues == [
        [IssueLocator(CINTable.ChildIdentifiers, CPPstartDate, 2), IssueLocator(CINTable.ChildIdentifiers, CPPendDate, 2),], # first instance of the issue
        [IssueLocator(CINTable.ChildIdentifiers, CPPstartDate, 4), IssueLocator(CINTable.ChildIdentifiers, CPPendDate, 4),], # second instance of the issue
    ]

    # Check that the rule definition is what you wrote in the context above.

    # replace 8925 with the rule code and put the appropriate message in its place too.
    assert result.definition.code == 8925
    assert result.definition.message == "Child Protection Plan End Date earlier than Start Date"

test_validate()