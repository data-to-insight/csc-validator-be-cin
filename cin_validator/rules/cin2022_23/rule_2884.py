from typing import Mapping

import pandas as pd

from cin_validator.rule_engine import CINTable, RuleContext, rule_definition
from cin_validator.test_engine import run_rule

CINdetails = CINTable.CINdetails
Section47 = CINTable.Section47

LAchildID = CINdetails.LAchildID
CINdetailsID = CINdetails.CINdetailsID
DateOfInitialCPC = Section47.DateOfInitialCPC
DateOfInitialCPC = CINdetails.DateOfInitialCPC


@rule_definition(
    code="2884",
    module=CINTable.Section47,
    message="An initial child protection conference is recorded at both the S47 and CIN Details level and it should only be recorded in one",
    affected_fields=[
        DateOfInitialCPC,
    ],
)
def validate(
    data_container: Mapping[CINTable, pd.DataFrame], rule_context: RuleContext
):
    df_47 = data_container[Section47].copy()
    df_cin = data_container[CINdetails].copy()

    df_47.index.name = "ROW_ID"
    df_cin.index.name = "ROW_ID"

    df_47.reset_index(inplace=True)
    df_cin.reset_index(inplace=True)

    merged_df = df_cin.merge(
        df_47,
        on=[LAchildID],
        suffixes=["_cin", "_47"],
    )

    condition = merged_df["DateOfInitialCPC_cin"] == merged_df["DateOfInitialCPC_47"]
    merged_df = merged_df[condition].reset_index()

    merged_df["ERROR_ID"] = tuple(
        zip(
            merged_df[LAchildID],
            merged_df["CINdetailsID_47"],
            merged_df["DateOfInitialCPC_cin"],
        )
    )

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
        table=CINdetails, columns=[DateOfInitialCPC], row_df=df_cin_issues
    )


def test_validate():
    sample_section47 = pd.DataFrame(
        [
            {  # 0 fail: datecpc == (child1, cinID2)'s datecpc in cindetails table
                "LAchildID": "child1",
                "DateOfInitialCPC": "26/05/2000",
                "CINdetailsID": "cinID1",
            },
            {  # 1 fail: datecpc == (child1, cinID2)'s datecpc in cindetails table
                "LAchildID": "child1",
                "DateOfInitialCPC": "26/05/2000",
                "CINdetailsID": "cinID2",
            },
            {  # 2
                "LAchildID": "child2",
                "DateOfInitialCPC": "30/05/2000",
                "CINdetailsID": "cinID1",
            },
            {  # 3
                "LAchildID": "child3",
                "DateOfInitialCPC": "27/05/2000",
                "CINdetailsID": "cinID1",
            },
            {  # 4 fail: datecpc == (child3, cinID2)'s datecpc in cindetails table
                "LAchildID": "child3",
                "DateOfInitialCPC": "26/05/2000",
                "CINdetailsID": "cinID2",
            },
            {  # 5 fail: datecpc == (child3, cinID2)'s datecpc in cindetails table
                "LAchildID": "child3",
                "DateOfInitialCPC": "26/05/2000",
                "CINdetailsID": "cinID3",
            },
            {  # 6
                "LAchildID": "child3",
                "DateOfInitialCPC": pd.NA,
                "CINdetailsID": "cinID4",
            },
        ]
    )
    sample_cin_details = pd.DataFrame(
        [
            {  # 0
                "LAchildID": "child1",
                "DateOfInitialCPC": "26/10/1999",
                "CINdetailsID": "cinID1",
            },
            {  # 1 fail
                "LAchildID": "child1",
                "DateOfInitialCPC": "26/05/2000",
                "CINdetailsID": "cinID2",
            },
            {  # 2
                "LAchildID": "child2",
                "DateOfInitialCPC": "26/05/2000",
                "CINdetailsID": "cinID1",
            },
            {  # 3
                "LAchildID": "child3",
                "DateOfInitialCPC": "28/05/2000",
                "CINdetailsID": "cinID1",
            },
            {  # 4 fail
                "LAchildID": "child3",
                "DateOfInitialCPC": "26/05/2000",
                "CINdetailsID": "cinID2",
            },
            {  # 5
                "LAchildID": "child3",
                "DateOfInitialCPC": "26/05/2003",
                "CINdetailsID": "cinID3",
            },
            {  # 6
                "LAchildID": "child3",
                "DateOfInitialCPC": "14/03/2001",
                "CINdetailsID": "cinID4",
            },
        ]
    )
    sample_section47["DateOfInitialCPC"] = pd.to_datetime(
        sample_section47["DateOfInitialCPC"], format="%d/%m/%Y", errors="coerce"
    )
    sample_cin_details["DateOfInitialCPC"] = pd.to_datetime(
        sample_cin_details["DateOfInitialCPC"], format="%d/%m/%Y", errors="coerce"
    )

    result = run_rule(
        validate,
        {
            Section47: sample_section47,
            CINdetails: sample_cin_details,
        },
    )

    issues_list = result.type2_issues
    assert len(issues_list) == 2
    issues = issues_list[0]

    issue_table = issues.table
    assert issue_table == Section47

    issue_columns = issues.columns
    assert issue_columns == [DateOfInitialCPC]

    issue_rows = issues.row_df
    assert len(issue_rows) == 4
    assert isinstance(issue_rows, pd.DataFrame)
    assert issue_rows.columns.to_list() == ["ERROR_ID", "ROW_ID"]

    expected_df = pd.DataFrame(
        [
            {
                "ERROR_ID": (
                    "child1",
                    "cinID1",
                    pd.to_datetime("26/05/2000", format="%d/%m/%Y", errors="coerce"),
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
            {
                "ERROR_ID": (
                    "child3",
                    "cinID2",
                    pd.to_datetime("26/05/2000", format="%d/%m/%Y", errors="coerce"),
                ),
                "ROW_ID": [4],
            },
            {
                "ERROR_ID": (
                    "child3",
                    "cinID3",
                    pd.to_datetime("26/05/2000", format="%d/%m/%Y", errors="coerce"),
                ),
                "ROW_ID": [5],
            },
        ]
    )
    assert issue_rows.equals(expected_df)

    assert result.definition.code == "2884"
    assert (
        result.definition.message
        == "An initial child protection conference is recorded at both the S47 and CIN Details level and it should only be recorded in one"
    )
