from typing import Mapping

import pandas as pd

from cin_validator.rule_engine import CINTable, RuleContext, RuleType, rule_definition
from cin_validator.test_engine import run_rule

Assessments = CINTable.Assessments
Section47 = CINTable.Section47


LAchildID = Assessments.LAchildID
CINdetailsID = Assessments.CINdetailsID


@rule_definition(
    code="2991Q",
    module=CINTable.CINdetails,
    rule_type=RuleType.QUERY,
    message="Please check and either amend data or provide a reason: A Section 47 module is recorded and there is no assessment on the episode",
    affected_fields=[
        CINdetailsID,
    ],
)
def validate(
    data_container: Mapping[CINTable, pd.DataFrame], rule_context: RuleContext
):
    df_ass = data_container[Assessments].copy()
    df_47 = data_container[Section47].copy()

    df_ass.index.name = "ROW_ID"
    df_47.index.name = "ROW_ID"

    df_ass.reset_index(inplace=True)
    df_47.reset_index(inplace=True)

    #  If <Section47> module is present then <Assessment> module should be present.
    merged_df = df_47.merge(
        df_ass,
        on=[LAchildID, CINdetailsID],
        suffixes=["_47", "_ass"],
        how="outer",
        indicator=True,
    )
    merged_df = merged_df[merged_df["_merge"] == "left_only"]

    merged_df["ERROR_ID"] = tuple(
        zip(
            merged_df[LAchildID],
            merged_df[CINdetailsID],
        )
    )
    df_ass_issues = (
        df_ass.merge(merged_df, left_on="ROW_ID", right_on="ROW_ID_ass")
        .groupby("ERROR_ID", group_keys=False)["ROW_ID"]
        .apply(list)
        .reset_index()
    )

    df_47_issues = (
        df_47.merge(merged_df, left_on="ROW_ID", right_on="ROW_ID_47")
        .groupby("ERROR_ID", group_keys=False)["ROW_ID"]
        .apply(list)
        .reset_index()
    )

    rule_context.push_type_2(
        table=Assessments, columns=[LAchildID], row_df=df_ass_issues
    )
    rule_context.push_type_2(table=Section47, columns=[LAchildID], row_df=df_47_issues)


def test_validate():
    sample_ass = pd.DataFrame(
        [
            {
                "LAchildID": "child1",
                "CPPstartDate": "26/05/2000",
                "CINdetailsID": pd.NA,
            },
            {
                "LAchildID": "child1",
                "CPPstartDate": "27/06/2002",
                "CINdetailsID": "cinID2",
            },
            {
                "LAchildID": "child3",
                "CPPstartDate": "26/05/2000",
                "CINdetailsID": "cinID1",
            },
            {
                "LAchildID": "child3",
                "CPPstartDate": pd.NA,
                "CINdetailsID": "cinID2",
            },
            {
                "LAchildID": "child3",
                "CPPstartDate": "07/02/2001",
                "CINdetailsID": "cinID3",
            },
            {
                "LAchildID": "child3",
                "CPPstartDate": "14/03/2001",
                "CINdetailsID": "cinID4",
            },
            {
                "LAchildID": "child4",
                "CPPstartDate": "14/03/2001",
                "CINdetailsID": "cinID4",
            },
        ]
    )
    sample_section47 = pd.DataFrame(
        [
            {
                "LAchildID": "child1",  # 0 fail. No assessment
                "DateOfInitialCPC": "26/05/2000",
                "CINdetailsID": "cinID1",
            },
            {
                "LAchildID": "child1",
                "DateOfInitialCPC": "26/05/2000",
                "CINdetailsID": "cinID2",
            },
            {
                "LAchildID": "child2",  # 2 fail. No assessment
                "DateOfInitialCPC": "30/05/2000",
                "CINdetailsID": "cinID1",
            },
            {
                "LAchildID": "child3",
                "DateOfInitialCPC": "27/05/2000",
                "CINdetailsID": "cinID1",
            },
            {
                "LAchildID": "child3",
                "DateOfInitialCPC": "26/05/2000",
                "CINdetailsID": "cinID2",
            },
            {
                "LAchildID": "child3",
                "DateOfInitialCPC": "26/05/2000",
                "CINdetailsID": "cinID3",
            },
            {
                "LAchildID": "child3",  # 6 fail. No assessment
                "DateOfInitialCPC": pd.NA,
                "CINdetailsID": pd.NA,
            },
        ]
    )

    result = run_rule(
        validate,
        {
            Assessments: sample_ass,
            Section47: sample_section47,
        },
    )

    issues_list = result.type2_issues
    assert len(issues_list) == 2
    issues = issues_list[1]

    issue_table = issues.table
    assert issue_table == Section47

    issue_columns = issues.columns
    assert issue_columns == [LAchildID]

    issue_rows = issues.row_df
    assert len(issue_rows) == 3
    assert isinstance(issue_rows, pd.DataFrame)
    assert issue_rows.columns.to_list() == ["ERROR_ID", "ROW_ID"]

    expected_df = pd.DataFrame(
        [
            {
                "ERROR_ID": (
                    "child1",
                    "cinID1",
                ),
                "ROW_ID": [0],
            },
            {
                "ERROR_ID": (
                    "child2",
                    "cinID1",
                ),
                "ROW_ID": [2],
            },
            {
                "ERROR_ID": (
                    "child3",
                    pd.NA,
                ),
                "ROW_ID": [6],
            },
        ]
    )
    assert issue_rows.equals(expected_df)

    assert result.definition.code == "2991Q"
    assert (
        result.definition.message
        == "Please check and either amend data or provide a reason: A Section 47 module is recorded and there is no assessment on the episode"
    )
