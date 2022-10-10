from cin_validator.rule_engine import RuleContext, IssueLocator, rule_definition, CINTable
from cin_validator.test_engine import run_rule
import pandas as pd
from typing import Mapping

ChildProtectionPlans = CINTable.ChildProtectionPlans
CPPstartDate = ChildProtectionPlans.CPPstartDate
CPPendDate = ChildProtectionPlans.CPPendDate

CINplanDates = CINTable.CINplanDates
CINPlanStartDate = CINplanDates.CINPlanStartDate

@rule_definition(code=123, 
                module=CINTable.ChildIdentifiers, 
                message="sample message here",
                affected_fields=["sample child"],
                )
def validate(data_container: Mapping[CINTable, pd.DataFrame], rule_context: RuleContext):
    """
    This test function checks for location linking among
    - multiple columns in the same table.
    - multiple columns across multiple tables where error occurences are mapped 1 to 1 and in order.

    At this point, this test function does not cover the use cases where
    - uneven lengths of error locations are returned across columns.
    - Failing child IDs are present in some tables and missing in others.
    - NaNs are present among the index values.
    """
    start_date_locs = pd.Index([1, 3, 5, 7])
    end_date_locs = pd.Index([2, 4, 6, 8])
    plan_start_locs = pd.Index([0, 1, 2, 3])

    cpp_id_col = pd.Index(["001", "009", "011", "012"])
    plan_id_col = pd.Index(["001", "009", "011", "012"]) # In an error instance, is it possible child ids per table to differ across tables?

    rule_context.push_linked_issues(
        [
            (ChildProtectionPlans, CPPstartDate, start_date_locs, cpp_id_col),
            (ChildProtectionPlans, CPPendDate, end_date_locs, cpp_id_col),
            (CINplanDates, CINPlanStartDate, plan_start_locs, plan_id_col),
        ]
    )

def test_context():
 
    child_protection_plans = pd.DataFrame([
        {"LAchildID": 1, "CPPstartDate": "26/05/2000" , "CPPendDate": "26/05/2000" ,},
        {"LAchildID": 2, "CPPstartDate": "26/05/2000" , "CPPendDate": "26/05/2001" ,},
        {"LAchildID": 3, "CPPstartDate": "26/05/2000" , "CPPendDate":  "26/05/1999",},
        {"LAchildID": 3, "CPPstartDate": "26/05/2000" , "CPPendDate": pd.NA,},
        {"LAchildID": 4, "CPPstartDate":  "26/05/2000" , "CPPendDate":  "25/05/2000",},
        {"LAchildID": 5, "CPPstartDate": pd.NA, "CPPendDate": pd.NA,},
    ])

    rule_context = run_rule(validate, {ChildProtectionPlans: child_protection_plans})
    print(rule_context.issues)
  
    assert rule_context.issues == [
        [IssueLocator(table=ChildProtectionPlans, field=CPPstartDate, row=1),
        IssueLocator(table=ChildProtectionPlans, field=CPPendDate, row=2),
        IssueLocator(table=CINplanDates, field=CINPlanStartDate, row=0),],

        [IssueLocator(table=ChildProtectionPlans, field=CPPstartDate, row=3),
        IssueLocator(table=ChildProtectionPlans, field=CPPendDate, row=4),
        IssueLocator(table=CINplanDates, field=CINPlanStartDate, row=1),],

        [IssueLocator(table=ChildProtectionPlans, field=CPPstartDate, row=5),
        IssueLocator(table=ChildProtectionPlans, field=CPPendDate, row=6),
        IssueLocator(table=CINplanDates, field=CINPlanStartDate, row=2),],

        [IssueLocator(table=ChildProtectionPlans, field=CPPstartDate, row=7),
        IssueLocator(table=ChildProtectionPlans, field=CPPendDate, row=8),
        IssueLocator(table=CINplanDates, field=CINPlanStartDate, row=3),],
    ]