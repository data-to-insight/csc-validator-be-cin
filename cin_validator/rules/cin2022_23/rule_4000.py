from typing import Mapping

import pandas as pd

from cin_validator.rule_engine import CINTable, RuleContext, rule_definition
from cin_validator.test_engine import run_rule

CINplanDates = CINTable.CINplanDates
LAchildID = CINplanDates.LAchildID

CINdetails = CINTable.CINdetails
ReferralNFA = CINdetails.ReferralNFA
CINdetailsID_details = CINdetails.CINdetailsID


@rule_definition(
    code="4000",
    module=CINTable.CINplanDates,
    message="CIN Plan details provided for a referral with no further action",
    affected_fields=[
        ReferralNFA,
    ],
)
def validate(
    data_container: Mapping[CINTable, pd.DataFrame], rule_context: RuleContext
):
    df_cpd = data_container[CINplanDates].copy()
    df_CINdetails = data_container[CINdetails].copy()

    df_cpd.index.name = "ROW_ID"
    df_CINdetails.index.name = "ROW_ID"

    df_cpd.reset_index(inplace=True)
    df_CINdetails.reset_index(inplace=True)

    df_CINdetails = df_CINdetails[
        (df_CINdetails[ReferralNFA] == "true")
        | (df_CINdetails[ReferralNFA] == 1)
        | (df_CINdetails[ReferralNFA] == "1")
        | (df_CINdetails[ReferralNFA] == True)
    ]

    df_merged = df_CINdetails.merge(
        df_cpd,
        left_on=["LAchildID", "CINdetailsID"],
        right_on=["LAchildID", "CINdetailsID"],
        how="inner",
        suffixes=("_cin", "_cpd"),
    )

    df_merged = df_merged.reset_index()

    df_merged["ERROR_ID"] = tuple(
        zip(
            df_merged["LAchildID"],
            df_merged["CINdetailsID"],
        )
    )

    df_cpp_issues = (
        df_cpd.merge(df_merged, left_on="ROW_ID", right_on="ROW_ID_cpd")
        .groupby("ERROR_ID", group_keys=False)["ROW_ID"]
        .apply(list)
        .reset_index()
    )
    df_cin_issues = (
        df_CINdetails.merge(df_merged, left_on="ROW_ID", right_on="ROW_ID_cin")
        .groupby("ERROR_ID", group_keys=False)["ROW_ID"]
        .apply(list)
        .reset_index()
    )

    rule_context.push_type_2(
        table=CINplanDates, columns=[LAchildID], row_df=df_cpp_issues
    )
    rule_context.push_type_2(
        table=CINdetails, columns=[ReferralNFA], row_df=df_cin_issues
    )


def test_validate():
    sample_cpd = pd.DataFrame(
        [
            {
                "LAchildID": "child1",
                "CINdetailsID": "CDID1",
            },
            {
                "LAchildID": "child1",
                "CINdetailsID": "CDID2",
            },
            {
                "LAchildID": "child4",
                "CINdetailsID": "CDID6",
            },
        ]
    )
    sample_cin = pd.DataFrame(
        [
            {
                "LAchildID": "child1",  # Fail, module present
                "CINdetailsID": "CDID1",
                "ReferralNFA": "1",
            },
            {
                "LAchildID": "child1",  # Pass, no referralNFA
                "CINdetailsID": "CDID2",
                "ReferralNFA": pd.NA,
            },
            {
                "LAchildID": "child3",  # Pass, no module
                "CINdetailsID": "CDID6",
                "ReferralNFA": "1",
            },
            {
                "LAchildID": "child4",  # Pass, referral NFA is false
                "CINdetailsID": "CDID6",
                "ReferralNFA": "false",
            },
        ]
    )

    result = run_rule(
        validate,
        {
            CINplanDates: sample_cpd,
            CINdetails: sample_cin,
        },
    )

    issues_list = result.type2_issues
    assert len(issues_list) == 2
    issues = issues_list[1]

    issue_table = issues.table
    assert issue_table == CINdetails

    issue_columns = issues.columns
    assert issue_columns == [ReferralNFA]

    issue_rows = issues.row_df
    assert len(issue_rows) == 1
    assert isinstance(issue_rows, pd.DataFrame)
    assert issue_rows.columns.to_list() == ["ERROR_ID", "ROW_ID"]

    expected_df = pd.DataFrame(
        [
            {
                "ERROR_ID": (
                    "child1",  # ChildID
                    "CDID1",
                ),
                "ROW_ID": [0],
            },
        ]
    )
    assert issue_rows.equals(expected_df)

    assert result.definition.code == "4000"
    assert (
        result.definition.message
        == "CIN Plan details provided for a referral with no further action"
    )
