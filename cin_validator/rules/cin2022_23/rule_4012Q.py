from typing import Mapping

import pandas as pd

from cin_validator.rule_engine import rule_definition, CINTable, RuleContext
from cin_validator.rule_engine import IssueLocator
from cin_validator.test_engine import run_rule

CINplanDates = CINTable.CINplanDates
CINPlanStartDate = CINplanDates.CINPlanStartDate

# define characteristics of rule
@rule_definition(
    code="4012Q",
    module=CINTable.CINplanDates,
    message="CIN Plan shown as starting and ending on the same day - please check",
    affected_fields=[CINPlanStartDate],
)
def validate(
    data_container: Mapping[CINTable, pd.DataFrame], rule_context: RuleContext
):
    df = data_container[CINplanDates]
    """
    Within a <CINPlanDates> group, <CINPlanStartDate> (N00689) should not be the same as the <CINPlanEndDate> (N00690)
    """
    
    df = df[df['CINPlanStartDate'] == df['CINPlanEndDate']]

    failing_indices = df.index

    rule_context.push_issue(
        table=CINplanDates, field=CINPlanStartDate, row=failing_indices
    )


def test_validate():
    starts = ['01-01-2020', '01-02-2020', '01-03-2020', '15-01-2020', pd.NA, '01-07-2020', '15-01-2020', pd.NA]
    ends  =  ['01-01-2020', '01-01-2020', '01-03-2020', '17-01-2020', pd.NA, '01-01-2020', '15-01-2020', '01-01-2020']
    fake_dataframe = pd.DataFrame({"CINPlanStartDate": starts, "CINPlanEndDate": ends})

    result = run_rule(validate, {CINplanDates: fake_dataframe})

    issues = list(result.issues)

    assert len(issues) == 3

    assert issues == [
        IssueLocator(CINTable.CINplanDates, CINPlanStartDate, 0),
        IssueLocator(CINTable.CINplanDates, CINPlanStartDate, 2),
        IssueLocator(CINTable.CINplanDates, CINPlanStartDate, 6),
    ]

    assert result.definition.code == "4012Q"
    assert (
        result.definition.message
        == "CIN Plan shown as starting and ending on the same day - please check"
    )
