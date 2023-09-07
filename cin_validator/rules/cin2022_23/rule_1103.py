from typing import Mapping

import pandas as pd

from cin_validator.rule_engine import (
    CINTable,
    IssueLocator,
    RuleContext,
    rule_definition,
)
from cin_validator.test_engine import run_rule

Assessments = CINTable.Assessments
AssessmentActualStartDate = Assessments.AssessmentActualStartDate
LAchildID = Assessments.LAchildID
Assdetailsid = Assessments.CINdetailsID

CINdetails = CINTable.CINdetails
CINreferralDate = CINdetails.CINreferralDate
LAchildID = CINdetails.LAchildID
CINdetailsID = CINdetails.CINdetailsID


@rule_definition(
    code="1103",
    module=CINTable.Assessments,
    message="The assessment start date cannot be before the referral date",
    affected_fields=[
        AssessmentActualStartDate,
        CINreferralDate,
    ],
)
def validate(
    data_container: Mapping[CINTable, pd.DataFrame], rule_context: RuleContext
):
    df_ass = data_container[Assessments].copy()
    df_refs = data_container[CINdetails].copy()

    df_ass.index.name = "ROW_ID"
    df_refs.index.name = "ROW_ID"

    df_ass.reset_index(inplace=True)
    df_refs.reset_index(inplace=True)

    # Where present, the <AssessmentActualStartDate> (N00159) should be on or after the <CINReferralDate> (N00100)
    # Issues dfs should return rows where Assessment Start Date is less than the Referral Start Date
    df_ass = df_ass[df_ass[AssessmentActualStartDate].notna()]
    df_refs = df_refs[df_refs[CINreferralDate].notna()]

    #  Merge tables to get corresponding Assessment group and referrals
    df_merged = df_ass.merge(
        df_refs,
        left_on=["LAchildID", "CINdetailsID"],
        right_on=["LAchildID", "CINdetailsID"],
        how="left",
        suffixes=("_ass", "_refs"),
    )

    #  Get rows where Assessment Start Date is less than the Referral Start Date
    condition = df_merged[AssessmentActualStartDate] < df_merged[CINreferralDate]
    df_merged = df_merged[condition].reset_index()

    df_merged["ERROR_ID"] = tuple(
        zip(
            df_merged[LAchildID],
            df_merged[AssessmentActualStartDate],
            df_merged[CINreferralDate],
        )
    )

    # The merges were done on copies of fs_ass and df_refs so that the column names in dataframes themselves aren't affected by the suffixes.
    # we can now map the suffixes columns to their corresponding source tables such that the failing ROW_IDs and ERROR_IDs exist per table.
    df_ass_issues = (
        df_ass.merge(df_merged, left_on="ROW_ID", right_on="ROW_ID_ass")
        .groupby("ERROR_ID", group_keys=False)["ROW_ID"]
        .apply(list)
        .reset_index()
    )
    df_refs_issues = (
        df_refs.merge(df_merged, left_on="ROW_ID", right_on="ROW_ID_refs")
        .groupby("ERROR_ID", group_keys=False)["ROW_ID"]
        .apply(list)
        .reset_index()
    )

    # Ensure that you maintain the ROW_ID, and ERROR_ID column names which are shown above. They are keywords in this project.
    rule_context.push_type_2(
        table=Assessments, columns=[AssessmentActualStartDate], row_df=df_ass_issues
    )
    rule_context.push_type_2(
        table=CINdetails, columns=[CINreferralDate], row_df=df_refs_issues
    )


def test_validate():
    sample_ass = pd.DataFrame(
        [
            {
                "LAchildID": "child1",
                "AssessmentActualStartDate": "30/06/2021",  # Fails as referral date is after assessment start
                "CINdetailsID": "CIN1",
            },
            {
                "LAchildID": "child2",
                "AssessmentActualStartDate": "10/09/2021",  #  Passes as assessment starts after referal start date
                "CINdetailsID": "CIN2",
            },
            {
                "LAchildID": "child3",
                "AssessmentActualStartDate": pd.NA,  # Ignored as no Assessment Date recorded
                "CINdetailsID": "CIN3",
            },
            {
                "LAchildID": "child4",
                "AssessmentActualStartDate": "01/12/2021",  # Fails as assessment starts after referral start date
                "CINdetailsID": "CIN4",
            },
            {
                "LAchildID": "child5",
                "AssessmentActualStartDate": "10/02/2022",  # Fails as no Referral Start Date recorded
                "CINdetailsID": "CIN5",
            },
        ]
    )
    sample_refs = pd.DataFrame(
        [
            {
                "LAchildID": "child1",  # Fails
                "CINreferralDate": "01/07/2021",
                "CINdetailsID": "CIN1",
            },
            {
                "LAchildID": "child2",  # Passes
                "CINreferralDate": "01/09/2021",
                "CINdetailsID": "CIN2",
            },
            {
                "LAchildID": "child3",  # Ignored
                "CINreferralDate": "26/05/2000",
                "CINdetailsID": "CIN3",
            },
            {
                "LAchildID": "child4",  # Fails
                "CINreferralDate": "10/12/2021",
                "CINdetailsID": "CIN4",
            },
            {
                "LAchildID": "child5",  # Ignored
                "CINreferralDate": pd.NA,
                "CINdetailsID": "CIN5",
            },
        ]
    )

    sample_ass[AssessmentActualStartDate] = pd.to_datetime(
        sample_ass[AssessmentActualStartDate], format="%d/%m/%Y", errors="coerce"
    )
    sample_refs["CINreferralDate"] = pd.to_datetime(
        sample_refs["CINreferralDate"], format="%d/%m/%Y", errors="coerce"
    )

    result = run_rule(
        validate,
        {
            Assessments: sample_ass,
            CINdetails: sample_refs,
        },
    )

    # Type 2 rule.
    issues_list = result.type2_issues
    assert len(issues_list) == 2
    issues = issues_list[1]

    issue_table = issues.table
    assert issue_table == CINdetails

    issue_columns = issues.columns
    assert issue_columns == [CINreferralDate]

    issue_rows = issues.row_df
    assert len(issue_rows) == 2
    assert isinstance(issue_rows, pd.DataFrame)
    assert issue_rows.columns.to_list() == ["ERROR_ID", "ROW_ID"]

    expected_df = pd.DataFrame(
        [
            {
                "ERROR_ID": (
                    "child1",  # ChildID
                    # Assessment Date
                    pd.to_datetime("30/06/2021", format="%d/%m/%Y", errors="coerce"),
                    # Referral date
                    pd.to_datetime("01/07/2021", format="%d/%m/%Y", errors="coerce"),
                ),
                "ROW_ID": [0],
            },
            {
                "ERROR_ID": (
                    "child4",  # ChildID
                    # Assessmwent date
                    pd.to_datetime("01/12/2021", format="%d/%m/%Y", errors="coerce"),
                    # Referral date
                    pd.to_datetime("10/12/2021", format="%d/%m/%Y", errors="coerce"),
                ),
                "ROW_ID": [3],
            },
        ]
    )
    assert issue_rows.equals(expected_df)

    assert result.definition.code == "1103"
    assert (
        result.definition.message
        == "The assessment start date cannot be before the referral date"
    )
