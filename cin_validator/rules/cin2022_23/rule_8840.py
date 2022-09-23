from typing import Mapping

import pandas as pd

from cin_validator.rule_engine import rule_definition, CINTable, RuleContext
from cin_validator.rule_engine import IssueLocator
from cin_validator.test_engine import run_rule

# Get tables and columns of interest from the CINTable object defined in rule_engine/__api.py
# Replace ChildIdentifiers with the table name, and LAChildID with the column name you want.

ChildProtectionPlans = CINTable.ChildProtectionPlans
CPPstartDate = ChildProtectionPlans.CPPstartDate
CPPendDate = ChildProtectionPlans.CPPendDate

# define characteristics of rule
@rule_definition(
    # write the rule code here, in place of 8500
    code=8840,
    # replace ChildIdentifiers with the value in the module column of the excel sheet corresponding to this rule .
    module=CINTable.ChildProtectionPlans,
    # replace the message with the corresponding value for this rule, gotten from the excel sheet.
    message="Child Protection Plan cannot start and end on the same day",
    # The column names tend to be the words within the < > signs in the github issue description.
    affected_fields=[ChildProtectionPlans, CPPstartDate, CPPendDate],
)
def validate(
    data_container: Mapping[CINTable, pd.DataFrame], rule_context: RuleContext
):
    # Replace ChildIdentifiers with the name of the table you need.
    df = data_container[ChildProtectionPlans]

    # implement rule logic as descriped by the Github issue. Put the description as a comment above the implementation as shown.

    #  Determine if the dates are the same by finding is the difference between dates is 0
    failing_indices = df[df['CPPstartDate'] == df['CPPendDate']].index

    # Replace ChildIdentifiers and LAchildID with the table and column name concerned in your rule, respectively. 
    # If there are multiple columns or table, make this sentence multiple times.
    rule_context.push_issue(
        table=ChildProtectionPlans, field=CPPstartDate, row=failing_indices
    )
    rule_context.push_issue(
        table=ChildProtectionPlans, field=CPPendDate, row=failing_indices
    )


def test_validate():
    # Create some sample data such that some values pass the validation and some fail.
    child_protection_plans = pd.DataFrame(
        {'CPPstartDate' : ['08/10/1989', '05/12/1993', '05/12/1993', '05/12/1997'], 
        'CPPendDate' : ['08/10/1989','05/12/1993', '12/09/2022', '05/12/1997']
        })
    child_protection_plans['CPPstartDate'] = pd.to_datetime(child_protection_plans['CPPstartDate'])
    child_protection_plans['CPPendDate'] = pd.to_datetime(child_protection_plans['CPPendDate'])
    
    # Run rule function passing in our sample data
    result = run_rule(validate, {ChildProtectionPlans: child_protection_plans})

    # The result contains a list of issues encountered
    issues = list(result.issues)
    # replace 2 with the number of failing points you expect from the sample data.
    assert len(issues) == 6
    # replace the table and column name as done earlier. 
    # The last numbers represent the index values where you expect the sample data to fail the validation check.
    assert issues == [
        IssueLocator(CINTable.ChildProtectionPlans, CPPstartDate, 0),
        IssueLocator(CINTable.ChildProtectionPlans, CPPstartDate, 1),
        IssueLocator(CINTable.ChildProtectionPlans, CPPstartDate, 3),
        IssueLocator(CINTable.ChildProtectionPlans, CPPendDate, 0),
        IssueLocator(CINTable.ChildProtectionPlans, CPPendDate, 1),
        IssueLocator(CINTable.ChildProtectionPlans, CPPendDate, 3),
    ]

    # Check that the rule definition is what you wrote in the context above.

    # replace 8500 with the rule code and put the appropriate message in its place too.
    assert result.definition.code == 8840
    assert result.definition.message == "Child Protection Plan cannot start and end on the same day"
