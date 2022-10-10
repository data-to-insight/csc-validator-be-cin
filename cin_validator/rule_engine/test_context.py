from cin_validator.rule_engine import RuleContext, IssueLocator, RuleDefinition, CINTable
import pandas as pd

def test_context():

    rule_definition = RuleDefinition(code=123, 
                                    module=CINTable.ChildIdentifiers, 
                                    message="sample message here",
                                    affected_fields=["sample child"],
                                    )
    rule_context = RuleContext(definition=rule_definition)
    ChildProtectionPlans = CINTable.ChildProtectionPlans
    CPPstartDate = CINTable.CPPstartDate
    CPPendDate = CINTable.CPPendDate

    CINplanDates = CINTable.CINplanDates
    CINPlanStartDate = CINTable.CINPlanStartDate

    start_date_locs = pd.index([1, 3, 5, 7])
    end_date_locs = pd.index([2, 4, 6, 8])
    plan_start_locs = pd.index([0, 1, 2, 3])

    cpp_id_col = pd.index(["001", "009", "011", "012"])
    plan_id_col = pd.index(["001", "009", "011", "012"]) # In an error instance, is it possible child ids per table to differ across tables?

    rule_context.push_linked_issues(
        [
            (ChildProtectionPlans, CPPstartDate, start_date_locs, cpp_id_col),
            (ChildProtectionPlans, CPPendDate, end_date_locs, cpp_id_col),
            (CINplanDates, CINPlanStartDate, plan_start_locs, plan_id_col),
        ]
    )
    
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
test_context()