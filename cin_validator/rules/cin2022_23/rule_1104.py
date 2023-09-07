from typing import Mapping

import pandas as pd

from cin_validator.rule_engine import CINTable, RuleContext, rule_definition
from cin_validator.test_engine import run_rule

Section47 = CINTable.Section47
DateOfInitialCPC = Section47.DateOfInitialCPC

CINdetails = CINTable.CINdetails
CINreferralDate = CINdetails.CINreferralDate
LAchildID = CINdetails.LAchildID
CINdetailsID = CINdetails.CINdetailsID


@rule_definition(
    code="1104",
    module=CINTable.Section47,
    message="The date of the initial child protection conference cannot be before the referral date",
    affected_fields=[
        CINreferralDate,
        DateOfInitialCPC,
    ],
)
def validate(
    data_container: Mapping[CINTable, pd.DataFrame], rule_context: RuleContext
):
    df_cin = data_container[CINdetails]
    df_47 = data_container[Section47]

    df_cin.index.name = "ROW_ID"
    df_47.index.name = "ROW_ID"

    df_cin.reset_index(inplace=True)
    df_47.reset_index(inplace=True)

    # Where present, the <DateOfInitialCPC> (N00110) should be on or after <CINreferralDate> (N00100)
    df_47 = df_47[df_47[DateOfInitialCPC].notna()]
    # get only relevant rows in df_47 (line above) and relevant columns in CIN
    # (line below: prevent the other DateOfInitialCPC from coming along in the merge else DateOfInitialCPC column name
    # will depend on whether the same name in present in the CINdetails table and that is out of scope for this rule.)
    df_cin_filtered = df_cin[["ROW_ID", LAchildID, CINdetailsID, CINreferralDate]]

    merged_df = df_47.copy().merge(
        df_cin_filtered,
        on=[LAchildID, CINdetailsID],
        how="left",
        suffixes=["_47", "_cin"],
    )

    # check that the the dates being compared existed in the same CIN event period and belong to the same child.
    condition = merged_df[DateOfInitialCPC] < merged_df[CINreferralDate]

    merged_df = merged_df[condition].reset_index()

    # create an identifier for each error instance.
    merged_df["ERROR_ID"] = tuple(
        zip(merged_df[LAchildID], merged_df[CINdetailsID], merged_df[DateOfInitialCPC])
    )

    # The merges were done on copies of df_47 and df_cin so that the column names in dataframes themselves aren't affected by the suffixes.
    # we can now map the suffixes columns to their corresponding source tables such that the failing ROW_IDs and ERROR_IDs exist per table.
    df_47_issues = (
        df_47.merge(merged_df, left_on="ROW_ID", right_on="ROW_ID_47")
        .groupby("ERROR_ID", group_keys=False)["ROW_ID"]
        .apply(list)
        .reset_index()
    )
    df_cin_issues = (
        df_cin.merge(merged_df, left_on="ROW_ID", right_on="ROW_ID_cin")
        .groupby("ERROR_ID", group_keys=False)["ROW_ID"]
        .apply(list)
        .reset_index()
    )

    rule_context.push_type_2(
        table=Section47, columns=[DateOfInitialCPC], row_df=df_47_issues
    )
    rule_context.push_type_2(
        table=CINdetails, columns=[CINreferralDate], row_df=df_cin_issues
    )


def test_validate():
    sample_section47 = pd.DataFrame(
        [
            {  # 0 fail: before
                "LAchildID": "child1",
                "CINdetailsID": "cinID1",
                "DateOfInitialCPC": "19/05/2000",
            },
            {  # 1 fail: before
                "LAchildID": "child1",
                "CINdetailsID": "cinID2",
                "DateOfInitialCPC": "26/05/2000",
            },
            {  # 2 ignore
                "LAchildID": "child2",
                "CINdetailsID": "cinID1",
                "DateOfInitialCPC": pd.NA,
            },
            {  # 3 pass: after
                "LAchildID": "child3",
                "CINdetailsID": "cinID1",
                "DateOfInitialCPC": "31/05/2003",
            },
            {  # 4 ignore: CINreferralDate is null
                "LAchildID": "child3",
                "CINdetailsID": "cinID2",
                "DateOfInitialCPC": "26/05/2000",
            },
        ]
    )
    sample_cin_details = pd.DataFrame(
        [
            {  # 0 fail
                "LAchildID": "child1",
                "CINdetailsID": "cinID1",
                "CINreferralDate": "26/10/2001",
            },
            {  # 1 fail
                "LAchildID": "child1",
                "CINdetailsID": "cinID2",
                "CINreferralDate": "13/06/2002",
            },
            {  # 2 ignore
                "LAchildID": "child2",
                "CINdetailsID": "cinID1",
                "CINreferralDate": "26/05/2000",
            },
            {  # 3 pass
                "LAchildID": "child3",
                "CINdetailsID": "cinID1",
                "CINreferralDate": "28/05/2000",
            },
            {  # 4 ignore
                "LAchildID": "child3",
                "CINdetailsID": "cinID2",
                "CINreferralDate": pd.NA,
            },
            {  # 5 ignore: doesn't match any ID when merged
                "LAchildID": "child3",
                "CINdetailsID": "cinID4",
                "CINreferralDate": "28/05/2000",
            },
        ]
    )
    sample_section47["DateOfInitialCPC"] = pd.to_datetime(
        sample_section47["DateOfInitialCPC"], format="%d/%m/%Y", errors="coerce"
    )
    sample_cin_details["CINreferralDate"] = pd.to_datetime(
        sample_cin_details["CINreferralDate"], format="%d/%m/%Y", errors="coerce"
    )

    result = run_rule(
        validate,
        {
            Section47: sample_section47,
            CINdetails: sample_cin_details,
        },
    )

    # Type 2 rule.
    issues_list = result.type2_issues
    assert len(issues_list) == 2
    issues = issues_list[0]

    issue_table = issues.table
    assert issue_table == Section47

    issue_columns = issues.columns
    assert issue_columns == [DateOfInitialCPC]

    issue_rows = issues.row_df
    assert len(issue_rows) == 2
    assert isinstance(issue_rows, pd.DataFrame)
    assert issue_rows.columns.to_list() == ["ERROR_ID", "ROW_ID"]

    expected_df = pd.DataFrame(
        [
            {
                "ERROR_ID": (
                    "child1",  # ChildID
                    "cinID1",  # CINdetailsID
                    # corresponding DateofInitialCPC
                    pd.to_datetime("19/05/2000", format="%d/%m/%Y", errors="coerce"),
                ),
                "ROW_ID": [0],
            },
            {
                "ERROR_ID": (
                    "child1",
                    "cinID2",
                    pd.to_datetime("26/05/2000", format="%d/%m/%Y", errors="coerce"),
                ),
                "ROW_ID": [1],
            },
        ]
    )
    assert issue_rows.equals(expected_df)

    assert result.definition.code == "1104"
    assert (
        result.definition.message
        == "The date of the initial child protection conference cannot be before the referral date"
    )
