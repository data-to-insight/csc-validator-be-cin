from typing import Mapping

import pandas as pd

from cin_validator.rule_engine import CINTable, RuleContext, rule_definition
from cin_validator.test_engine import run_rule

ChildProtectionPlans = CINTable.ChildProtectionPlans
LAchildID = ChildProtectionPlans.LAchildID
CINdetailsID = ChildProtectionPlans.CINdetailsID
CPPID = ChildProtectionPlans.CPPID
CPPendDate = ChildProtectionPlans.CPPendDate

CINplanDates = CINTable.CINplanDates
CINPlanEndDate = CINplanDates.CINPlanEndDate
CINPlanStartDate = CINplanDates.CINPlanStartDate


@rule_definition(
    code="4001",
    module=CINTable.CINplanDates,
    message="A CIN Plan cannot run concurrently with a Child Protection Plan",
    affected_fields=[
        CINPlanEndDate,
        CPPendDate,
    ],
)
def validate(
    data_container: Mapping[CINTable, pd.DataFrame], rule_context: RuleContext
):
    df_cpp = data_container[ChildProtectionPlans]
    df_plan = data_container[CINplanDates]

    df_cpp.index.name = "ROW_ID"
    df_plan.index.name = "ROW_ID"

    df_cpp.reset_index(inplace=True)
    df_plan.reset_index(inplace=True)

    # If a <CINDetails> module has a <ChildProtectionPlan> module present with no <CPPendDate> (N00115)
    # - then a <CINPlanDates> module with no <CINPlanEndDate> (N00690) must not be present
    df_merged = df_cpp.merge(
        df_plan,
        on=["LAchildID", "CINdetailsID"],
        how="left",
        suffixes=("_cpp", "_cin"),
    )

    #  Get rows where CPPendDate is null and CINPlanEndDate is null
    condition = df_merged[CPPendDate].isna() & (
        df_merged[CINPlanStartDate].notna() & df_merged[CINPlanEndDate].isna()
    )
    df_merged = df_merged[condition].reset_index()

    df_merged["ERROR_ID"] = tuple(zip(df_merged[LAchildID], df_merged[CINdetailsID]))

    df_cpp_issues = (
        df_cpp.merge(df_merged, left_on="ROW_ID", right_on="ROW_ID_cpp")
        .groupby("ERROR_ID", group_keys=False)["ROW_ID"]
        .apply(list)
        .reset_index()
    )
    df_cin_issues = (
        df_plan.merge(df_merged, left_on="ROW_ID", right_on="ROW_ID_cin")
        .groupby("ERROR_ID", group_keys=False)["ROW_ID"]
        .apply(list)
        .reset_index()
    )
    rule_context.push_type_2(
        table=ChildProtectionPlans,
        columns=[CPPendDate],
        row_df=df_cpp_issues,
    )
    rule_context.push_type_2(
        table=CINplanDates, columns=[CINPlanEndDate], row_df=df_cin_issues
    )


def test_validate():
    sample_cpp = pd.DataFrame(
        [
            {
                "LAchildID": "child1",  # 0 Pass
                "CPPendDate": "30/05/2000",
                "CINdetailsID": "cinID1",
            },
            {
                "LAchildID": "child1",  # 1 Pass
                "CPPendDate": pd.NA,
                "CINdetailsID": "cinID1",
            },
            {
                "LAchildID": "child2",  # 2 Fail
                "CPPendDate": pd.NA,
                "CINdetailsID": "cinID3",
            },
            {
                "LAchildID": "child3",  # 3 Pass
                "CPPendDate": "30/10/2001",
                "CINdetailsID": "cinID5",
            },
            {
                "LAchildID": "child5",  # 4 Pass
                "CPPendDate": pd.NA,
                "CINdetailsID": "cinID7",
            },
            {
                "LAchildID": "child6",  # 4 Pass
                "CPPendDate": pd.NA,
                "CINdetailsID": "cinID8",
            },
        ]
    )
    sample_plan = pd.DataFrame(
        [
            {
                "LAchildID": "child1",  # 0 Pass
                "CINPlanEndDate": "04/04/2000",
                "CINPlanStartDate": "04/04/2000",
                "CINdetailsID": "cinID1",
            },
            {
                "LAchildID": "child1",  # 1 Pass
                "CINPlanEndDate": "28/05/2000",
                "CINPlanStartDate": "04/04/2000",
                "CINdetailsID": "cinID1",
            },
            {
                "LAchildID": "child2",  # 2 Fail
                "CINPlanStartDate": "04/04/2000",
                "CINPlanEndDate": pd.NA,
                "CINdetailsID": "cinID3",
            },
            {
                "LAchildID": "child2",  # 3 Pass
                "CINPlanStartDate": "04/04/2000",
                "CINPlanEndDate": pd.NA,
                "CINdetailsID": "cinID4",
            },
            {
                "LAchildID": "child3",  # 4 Pass
                "CINPlanStartDate": "04/04/2000",
                "CINPlanEndDate": "30/10/2001",
                "CINdetailsID": "cinID5",
            },
            {
                "LAchildID": "child4",  # 5 Pass
                "CINPlanStartDate": "04/04/2000",
                "CINPlanEndDate": pd.NA,
                "CINdetailsID": "cinID6",
            },
            {
                "LAchildID": "child6",  # 6 Pass
                "CINPlanEndDate": pd.NA,
                "CINdetailsID": "cinID9",
            },
        ]
    )

    sample_cpp[CPPendDate] = pd.to_datetime(
        sample_cpp[CPPendDate], format="%d/%m/%Y", errors="coerce"
    )
    sample_plan[CINPlanEndDate] = pd.to_datetime(
        sample_plan[CINPlanEndDate], format="%d/%m/%Y", errors="coerce"
    )

    result = run_rule(
        validate,
        {
            ChildProtectionPlans: sample_cpp,
            CINplanDates: sample_plan,
        },
    )
    issues_list = result.type2_issues
    assert len(issues_list) == 2
    issues = issues_list[1]

    issue_table = issues.table
    assert issue_table == CINplanDates

    issue_columns = issues.columns
    assert issue_columns == [CINPlanEndDate]

    issue_rows = issues.row_df
    assert len(issue_rows) == 1
    assert isinstance(issue_rows, pd.DataFrame)
    assert issue_rows.columns.to_list() == ["ERROR_ID", "ROW_ID"]

    expected_df = pd.DataFrame(
        [
            {
                "ERROR_ID": (
                    "child2",
                    "cinID3",
                ),
                "ROW_ID": [2],
            },
        ]
    )
    assert issue_rows.equals(expected_df)

    assert result.definition.code == "4001"
    assert (
        result.definition.message
        == "A CIN Plan cannot run concurrently with a Child Protection Plan"
    )
