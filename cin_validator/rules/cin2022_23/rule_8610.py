from typing import Mapping

import pandas as pd

from cin_validator.rule_engine import CINTable, RuleContext, rule_definition
from cin_validator.test_engine import run_rule

CINdetails = CINTable.CINdetails
LAchildID = CINdetails.LAchildID
ReferralNFA = CINdetails.ReferralNFA
PrimaryNeedCode = CINdetails.PrimaryNeedCode
CINdetailsID = CINdetails.CINdetailsID


@rule_definition(
    code="8610",
    module=CINTable.CINdetails,
    message="Primary Need code is missing for a referral which led to further action.",
    affected_fields=[ReferralNFA, PrimaryNeedCode],
)
def validate(
    data_container: Mapping[CINTable, pd.DataFrame], rule_context: RuleContext
):
    df = data_container[CINdetails]
    df.index.name = "ROW_ID"
    df.reset_index(inplace=True)

    # If <ReferralNFA> (N00112) = false or 0
    # then
    # <PrimaryNeedCode> (N00101) must be present

    condition = (df[ReferralNFA].isin(["false", "0"])) & (df[PrimaryNeedCode].isna())
    df_issues = df[condition].reset_index()

    link_id = tuple(zip(df_issues[LAchildID], df_issues[CINdetailsID]))
    df_issues["ERROR_ID"] = link_id
    df_issues = (
        df_issues.groupby("ERROR_ID", group_keys=False)["ROW_ID"]
        .apply(list)
        .reset_index()
    )
    rule_context.push_type_1(
        table=CINdetails, columns=[ReferralNFA, PrimaryNeedCode], row_df=df_issues
    )


def test_validate():
    #  Fails rows 0, 1, and 3
    sample_cin = pd.DataFrame(
        {
            "LAchildID": ["child1", "child2", "child3", "child4", "child5"],
            "ReferralNFA": [
                "false",
                "0",
                "1",
                pd.NA,
                "true",
            ],
            "PrimaryNeedCode": [
                pd.NA,  # fail
                pd.NA,  # fail
                "12/09/2022",
                "05/12/1997",
                pd.NA,  # ignore: ReferralNFA is true
            ],
            "CINdetailsID": [
                "ID1",
                "ID2",
                "ID3",
                "ID4",
                "ID5",
            ],
        }
    )

    result = run_rule(validate, {CINdetails: sample_cin})

    issues = result.type1_issues

    issue_table = issues.table
    assert issue_table == CINdetails
    issue_columns = issues.columns
    assert issue_columns == [ReferralNFA, PrimaryNeedCode]

    issue_rows = issues.row_df

    assert len(issue_rows) == 2
    assert isinstance(issue_rows, pd.DataFrame)
    assert issue_rows.columns.to_list() == ["ERROR_ID", "ROW_ID"]

    expected_df = pd.DataFrame(
        [
            {
                "ERROR_ID": (
                    "child1",
                    "ID1",
                ),
                "ROW_ID": [0],
            },
            {
                "ERROR_ID": ("child2", "ID2"),
                "ROW_ID": [1],
            },
        ]
    )
    assert issue_rows.equals(expected_df)

    assert result.definition.code == "8610"
    assert (
        result.definition.message
        == "Primary Need code is missing for a referral which led to further action."
    )
