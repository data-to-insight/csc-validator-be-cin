from typing import Mapping

import pandas as pd

from cin_validator.rule_engine import rule_definition, CINTable, RuleContext
from cin_validator.rule_engine import IssueLocator
from cin_validator.test_engine import run_rule

Section47 = CINTable.Section47
CINdetails = CINTable.CINdetails
S47ActualStartDate = Section47.S47ActualStartDate

# define characteristics of rule
@rule_definition(
    code=2889,
    module=CINTable.Section47,
    message="The S47 start date cannot be before the referral date.",
    affected_fields=[S47ActualStartDate],
)
def validate(
    data_container: Mapping[CINTable, pd.DataFrame], rule_context: RuleContext
):
    """
    Where present, the <S47ActualStartDate> (N00148) should be on or after the <CINReferralDate> (N00100)
    """
    df_s47 = data_container[Section47]
    df_CIN = data_container[CINdetails]

    df_s47["S47ActualStartDate"] = pd.to_datetime(df_s47["S47ActualStartDate"], format=r"%Y-%m-%d", errors="coerce")
    df_CIN["CINreferralDate"] = pd.to_datetime(df_CIN["CINreferralDate"], format=r"%Y-%m-%d", errors="coerce")

    df_s47 = df_s47[["LAchildID", "CINdetailsID", "S47ActualStartDate"]]
    df_CIN = df_CIN[["LAchildID", "CINdetailsID", "CINreferralDate"]]
    df_s47.reset_index(inplace=True)

    #Merge tables via LAchildID and CINdetailsID
    df_merg = df_s47.merge(df_CIN, how="inner", on=["LAchildID", "CINdetailsID"], suffixes=["", "_cin"])

    #Remove null S47Starts
    df_merg = df_merg[df_merg["S47ActualStartDate"].notna()]

    #Check for S47 Start < Cin Ref date which are the error rows
    df_merg = df_merg[df_merg["S47ActualStartDate"] < df_merg["CINreferralDate"]]

    failing_indices = df_merg.set_index("index").index

    rule_context.push_issue(
        table=Section47, field=S47ActualStartDate, row=failing_indices
    )


def test_validate():
    s47_data = (
        #ID     #CINID    #S47 Date
        ('1',   '45',   '2020-05-05'), #0
        ('4',   '55',   '2019-04-20'), #1
        ('67',  '66',   '2014-03-21'), #2 Error preceeds 2016-03-21 in #C
        ('69',  '67',   '2018-04-20'), #3
        ('69',  '67',   pd.NA       ), #4
        ('167', '166',  '2014-03-21'), #5 Error preceeds 2015-02-21 in #G
    )

    cin_data = (
        #ID     #CINID   #CIN Ref Date
        ('1',   '44',   '2017-05-05'), #A
        ('4',   '55',   '2019-04-20'), #B
        ('67',  '66',   '2016-03-21'), #C
        ('67',  '67',   '2015-03-21'), #D
        ('69',  '67',   '2018-04-20'), #E
        ('70',  '69',   '2015-04-20'), #F
        ('167', '166',  '2015-02-21'), #G
    )

    fake_s47 = pd.DataFrame({"LAchildID": [x[0] for x in s47_data], "CINdetailsID": [x[1] for x in s47_data], "S47ActualStartDate": [x[2] for x in s47_data]})
    fake_cin = pd.DataFrame({"LAchildID": [x[0] for x in cin_data], "CINdetailsID": [x[1] for x in cin_data], "CINreferralDate": [x[2] for x in cin_data]})

    result = run_rule(validate, {Section47: fake_s47, CINdetails: fake_cin})

    issues = list(result.issues)

    assert len(issues) == 2

    assert issues == [
        IssueLocator(CINTable.Section47, S47ActualStartDate, 2),
        IssueLocator(CINTable.Section47, S47ActualStartDate, 5),
    ]

    assert result.definition.code == 2889
    assert (
        result.definition.message
        == "The S47 start date cannot be before the referral date."
    )