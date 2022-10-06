from typing import Mapping

import pandas as pd

from cin_validator.rule_engine import rule_definition, CINTable, RuleContext
from cin_validator.rule_engine import IssueLocator
from cin_validator.test_engine import run_rule

CINDe = CINTable.CINdetails
PriNeedC = CINDe.PrimaryNeedCode

# define characteristics of rule
@rule_definition(
    code=8610,
    module=CINTable.CINdetails,
    message="Primary Need code is missing for a referral which led to further action",
    affected_fields=[PriNeedC],
)
def validate(
    data_container: Mapping[CINTable, pd.DataFrame], rule_context: RuleContext
):
    df = data_container[CINDe]
    """
    If <ReferralNFA> (N00112) = false or 0 then
    <PrimaryNeedCode> (N00101) must be present
    """
    #RefNFA is false or 0
    df = df[(df['ReferralNFA'].str.lower() == 'false') | (df['ReferralNFA'].astype(str) == '0')]
    #If primary need is null we get the error rows
    df = df[df['PrimaryNeedCode'].isna()]

    failing_indices = df.index

    rule_context.push_issue(
        table=CINDe, field=PriNeedC, row=failing_indices
    )

def test_validate():
                   #0     #1     #2    #3     #4     #5     #6
    refNFA =   ['false','false', '1', '0',  'false', '2',  '1']
    PNeedCode = ['a',   pd.NA,   'b', pd.NA, 'c',   pd.NA, 'd']

    fake_df = pd.DataFrame({"ReferralNFA" : refNFA, "PrimaryNeedCode": PNeedCode})

    result = run_rule(validate, {CINDe: fake_df})

    issues = list(result.issues)

    assert len(issues) == 2

    assert issues == [
        IssueLocator(CINTable.CINdetails, PriNeedC, 1),
        IssueLocator(CINTable.CINdetails, PriNeedC, 3),
    ]

    assert result.definition.code == 8610
    assert result.definition.message == "Primary Need code is missing for a referral which led to further action"
