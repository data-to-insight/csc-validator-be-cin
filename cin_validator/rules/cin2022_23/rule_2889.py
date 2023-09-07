from typing import Mapping

import pandas as pd

from cin_validator.rule_engine import CINTable, RuleContext, rule_definition
from cin_validator.test_engine import run_rule

Section47 = CINTable.Section47
S47ActualStartDate = Section47.S47ActualStartDate

CINdetails = CINTable.CINdetails
CINreferralDate = CINdetails.CINreferralDate
LAchildID = CINdetails.LAchildID


@rule_definition(
    code="2889",
    module=CINTable.Section47,
    message="The S47 start date cannot be before the referral date.",
    affected_fields=[S47ActualStartDate, CINreferralDate],
)
def validate(
    data_container: Mapping[CINTable, pd.DataFrame], rule_context: RuleContext
):
    df_s47 = data_container[Section47]
    df_cin = data_container[CINdetails]

    df_s47.index.name = "ROW_ID"
    df_cin.index.name = "ROW_ID"

    df_s47.reset_index(inplace=True)
    df_cin.reset_index(inplace=True)

    # Where present, the <S47ActualStartDate> (N00148) should be on or after the <CINReferralDate> (N00100)
    # Remove null S47Starts
    df_s47 = df_s47[df_s47[S47ActualStartDate].notna()]

    # Merge tables via LAchildID and CINdetailsID.
    df_merged = df_s47.merge(
        df_cin, how="left", on=["LAchildID", "CINdetailsID"], suffixes=["_47", "_cin"]
    )

    # Check for S47 Start < Cin Ref date which are the error rows
    condition = df_merged[S47ActualStartDate] < df_merged[CINreferralDate]
    df_merged = df_merged[condition].reset_index()

    df_merged["ERROR_ID"] = tuple(
        zip(
            df_merged[LAchildID],
            df_merged[S47ActualStartDate],
            df_merged[CINreferralDate],
        )
    )

    df_47_issues = (
        df_s47.merge(df_merged, left_on="ROW_ID", right_on="ROW_ID_47")
        .groupby("ERROR_ID", group_keys=False)["ROW_ID"]
        .apply(list)
        .reset_index()
    )
    df_cin_issues = (
        df_cin.merge(df_merged, left_on="ROW_ID", right_on="ROW_ID_cin")
        .groupby("ERROR_ID", group_keys=False)["ROW_ID"]
        .apply(list)
        .reset_index()
    )

    rule_context.push_type_2(
        table=CINdetails, columns=[CINreferralDate], row_df=df_cin_issues
    )
    rule_context.push_type_2(
        table=Section47, columns=[S47ActualStartDate], row_df=df_47_issues
    )


def test_validate():
    s47_data = (
        # ID     #CINID    #S47 Date
        ("1", "45", "2020-05-05"),  # 0
        ("4", "55", "2019-04-20"),  # 1
        ("67", "66", "2014-03-21"),  # 2 fail: preceeds 2016-03-21 in #C
        ("69", "67", "2018-04-20"),  # 3
        ("69", "67", pd.NA),  # 4
        ("167", "166", "2014-03-21"),  # 5 fail: preceeds 2015-02-21 in #G
    )

    cin_data = (
        # ID     #CINID   #CIN Ref Date
        ("1", "44", "2017-05-05"),  # A
        ("4", "55", "2019-04-20"),  # B
        ("67", "66", "2016-03-21"),  # C fail
        ("67", "67", "2015-03-21"),  # D
        ("69", "67", "2018-04-20"),  # E
        ("70", "69", "2015-04-20"),  # F
        ("167", "166", "2015-02-21"),  # G fail
    )

    fake_s47 = pd.DataFrame(
        {
            "LAchildID": [x[0] for x in s47_data],
            "CINdetailsID": [x[1] for x in s47_data],
            S47ActualStartDate: [x[2] for x in s47_data],
        }
    )
    fake_cin = pd.DataFrame(
        {
            "LAchildID": [x[0] for x in cin_data],
            "CINdetailsID": [x[1] for x in cin_data],
            CINreferralDate: [x[2] for x in cin_data],
        }
    )

    fake_s47[S47ActualStartDate] = pd.to_datetime(
        fake_s47[S47ActualStartDate], format=r"%Y-%m-%d", errors="coerce"
    )
    fake_cin[CINreferralDate] = pd.to_datetime(
        fake_cin[CINreferralDate], format=r"%Y-%m-%d", errors="coerce"
    )

    result = run_rule(validate, {Section47: fake_s47, CINdetails: fake_cin})

    issues_list = result.type2_issues
    assert len(issues_list) == 2

    issues = issues_list[1]
    assert issues.table == Section47
    assert issues.columns == [S47ActualStartDate]

    issue_rows = issues.row_df
    assert len(issue_rows) == 2
    assert isinstance(issue_rows, pd.DataFrame)
    assert issue_rows.columns.to_list() == ["ERROR_ID", "ROW_ID"]

    expected_df = pd.DataFrame(
        [
            {
                "ERROR_ID": (
                    "167",  # ChildID
                    pd.to_datetime(
                        "2014-03-21", format=r"%Y-%m-%d", errors="coerce"
                    ),  # S47ActualStartDate
                    pd.to_datetime(
                        "2015-02-21", format=r"%Y-%m-%d", errors="coerce"
                    ),  # CINreferralDate
                ),
                "ROW_ID": [5],
            },
            {
                "ERROR_ID": (
                    "67",  # ChildID
                    pd.to_datetime(
                        "2014-03-21", format=r"%Y-%m-%d", errors="coerce"
                    ),  # S47ActualStartDate
                    pd.to_datetime(
                        "2016-03-21", format=r"%Y-%m-%d", errors="coerce"
                    ),  # CINreferralDate
                ),
                "ROW_ID": [2],
            },
        ]
    )
    assert issue_rows.equals(expected_df)

    assert result.definition.code == "2889"
    assert (
        result.definition.message
        == "The S47 start date cannot be before the referral date."
    )
