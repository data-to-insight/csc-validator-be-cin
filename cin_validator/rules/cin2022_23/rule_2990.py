from typing import Mapping

import pandas as pd

from cin_validator.rule_engine import CINTable, RuleContext, rule_definition
from cin_validator.test_engine import run_rule
from cin_validator.utils import make_census_period

ChildProtectionPlans = CINTable.ChildProtectionPlans
CINdetails = CINTable.CINdetails
Section47 = CINTable.Section47
CINplanDates = CINTable.CINplanDates

ReasonForClosure = CINdetails.ReasonForClosure
LAchildID = ChildProtectionPlans.LAchildID
CINdetailsID = ChildProtectionPlans.CINdetailsID
DateOfInitialCPC = CINdetails.DateOfInitialCPC


# define characteristics of rule
@rule_definition(
    code="2990",
    module=CINTable.CINdetails,
    message="Activity is recorded against a case marked as ‘Case closed after assessment, no further action’ or 'case closed after assessment, referred to early help'.",
    affected_fields=[
        ReasonForClosure,
    ],
)
def validate(
    data_container: Mapping[CINTable, pd.DataFrame], rule_context: RuleContext
):
    df_cpp = data_container[ChildProtectionPlans].copy()
    df_47 = data_container[Section47].copy()
    df_cin = data_container[CINdetails].copy()
    df_cin_pd = data_container[CINplanDates].copy()

    df_cpp.index.name = "ROW_ID"
    df_47.index.name = "ROW_ID"
    df_cin.index.name = "ROW_ID"
    df_cin_pd.index.name = "ROW_ID"

    df_cpp.reset_index(inplace=True)
    df_47.reset_index(inplace=True)
    df_cin.reset_index(inplace=True)
    df_cin_pd.reset_index(inplace=True)

    # If a <CINDetails> module has <ReasonForClosure> (N00103) = RC8 or RC9, then it cannot have any of the following modules:
    # <Section47> module
    # <ChildProtectionPlan> module
    # <DateofInitialCPC> (N00110) within the <CINDetails> module
    # <CINPlanDates> module
    df_cin = df_cin[df_cin[ReasonForClosure].isin(["RC8", "RC9"])]

    df_cin_cpp = df_cin.merge(
        df_cpp, on=["LAchildID", "CINdetailsID"], how="left", suffixes=["_cin", "_cpp"]
    )

    df_cin_47 = df_cin.merge(
        df_47,
        on=[
            "LAchildID",
            "CINdetailsID",
        ],
        how="left",
        suffixes=["_cin", "_47"],
    )

    df_cin_cin_pd = df_cin.merge(
        df_cin_pd,
        on=["LAchildID", "CINdetailsID"],
        how="left",
        suffixes=["_cin", "_pd"],
    )

    df_cin_cpp_47 = df_cin_cpp.merge(
        df_cin_47,
        left_on=[
            "LAchildID",
            "CINdetailsID",
            "ROW_ID_cin",
            "DateOfInitialCPC",
            "ReasonForClosure",
        ],
        right_on=[
            "LAchildID",
            "CINdetailsID",
            "ROW_ID_cin",
            "DateOfInitialCPC_cin",
            "ReasonForClosure",
        ],
        how="left",
        suffixes=["_cin_cpp", "_cin_47"],
    )

    # This merge uses the DateOfInitialCPC values from the original CIN table using their different suffixes from each merge
    merged_df = df_cin_cpp_47.merge(
        df_cin_cin_pd,
        left_on=[
            "LAchildID",
            "CINdetailsID",
            "ROW_ID_cin",
            "DateOfInitialCPC_cin",
            "ReasonForClosure",
        ],
        right_on=[
            "LAchildID",
            "CINdetailsID",
            "ROW_ID_cin",
            "DateOfInitialCPC",
            "ReasonForClosure",
        ],
        how="left",
        suffixes=["_cin_cpp_47", "_cin_cin_pd"],
    )

    # Logical conditions - other than this, of the tables can merge, it means there's modules and they are in error
    # Checks for the DateOfInitialCPC caqrried on through from the original CINdetails table
    condition_1 = merged_df["DateOfInitialCPC_cin"].notna()
    condition_2 = merged_df["ROW_ID_cpp"].notna()
    condition_3 = merged_df["ROW_ID_47"].notna()
    condition_4 = merged_df["ROW_ID_pd"].notna()

    merged_df = merged_df[
        condition_1 | condition_2 | condition_3 | condition_4
    ].reset_index()

    merged_df["ERROR_ID"] = tuple(zip(merged_df[LAchildID], merged_df[CINdetailsID]))

    df_cpp_issues = (
        df_cpp.merge(merged_df, left_on="ROW_ID", right_on="ROW_ID_cpp")
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
    df_cin_issues = (
        df_cin.merge(merged_df, left_on="ROW_ID", right_on="ROW_ID_cin")
        .groupby("ERROR_ID", group_keys=False)["ROW_ID"]
        .apply(list)
        .reset_index()
    )

    df_cin_pd_issues = (
        df_cin_pd.merge(merged_df, left_on="ROW_ID", right_on="ROW_ID_pd")
        .groupby("ERROR_ID", group_keys=False)["ROW_ID"]
        .apply(list)
        .reset_index()
    )

    rule_context.push_type_2(
        table=ChildProtectionPlans, columns=[LAchildID], row_df=df_cpp_issues
    )
    rule_context.push_type_2(
        table=CINdetails, columns=[DateOfInitialCPC], row_df=df_cin_issues
    )
    rule_context.push_type_2(
        table=CINplanDates, columns=[LAchildID], row_df=df_cin_pd_issues
    )
    rule_context.push_type_2(table=Section47, columns=[LAchildID], row_df=df_47_issues)


def test_validate():
    sample_cpp = pd.DataFrame(
        [
            {
                "LAchildID": "child1",
                "CPPstartDate": "26/05/2000",
                "CINdetailsID": "cinID10",
            },
            {
                "LAchildID": "child1",
                "CPPstartDate": "27/06/2002",
                "CINdetailsID": "cinID2",
            },
            {  # fails for having a module
                "LAchildID": "child3",
                "CPPstartDate": "26/05/2000",
                "CINdetailsID": "cinID3",
            },
            {
                "LAchildID": "child30",
                "CPPstartDate": "26/05/2000",
                "CINdetailsID": "cinID3",
            },
            {
                "LAchildID": "child30",
                "CPPstartDate": pd.NA,
                "CINdetailsID": "cinID2",
            },
            {
                "LAchildID": "child3",
                "CPPstartDate": "07/02/2001",
                "CINdetailsID": "cinID30",
            },
            {
                "LAchildID": "child3",
                "CPPstartDate": "14/03/2001",
                "CINdetailsID": "cinID4",
            },
        ]
    )
    sample_section47 = pd.DataFrame(
        [
            {
                "LAchildID": "child1",
                "DateOfInitialCPC": pd.NA,
                "CINdetailsID": "cinID2",
            },
            {
                "LAchildID": "child2",  # fails for having a module
                "DateOfInitialCPC": pd.NA,
                "CINdetailsID": "cinID2",
            },
            {
                "LAchildID": "child2",
                "DateOfInitialCPC": pd.NA,
                "CINdetailsID": "cinID1",
            },
            {
                "LAchildID": "child3",
                "DateOfInitialCPC": pd.NA,
                "CINdetailsID": "cinID1",
            },
            {
                "LAchildID": "child3",
                "DateOfInitialCPC": "26/05/2000",
                "CINdetailsID": "cinID2",
            },
            {
                "LAchildID": "child3",
                "DateOfInitialCPC": pd.NA,
                "CINdetailsID": "cinID3",
            },
            {
                "LAchildID": "child3",
                "DateOfInitialCPC": "14/03/2001",
                "CINdetailsID": "cinID4",
            },
        ]
    )
    sample_cin_details = pd.DataFrame(
        [
            {
                "LAchildID": "child1",
                "DateOfInitialCPC": pd.NA,  # 0 fail: found in CINplanDates table
                "CINdetailsID": "cinID1",
                "ReasonForClosure": "RC8",
            },
            {
                "LAchildID": "child2",
                "DateOfInitialCPC": pd.NA,
                "CINdetailsID": "cinID2",  # 1 fail: found in Section47 table
                "ReasonForClosure": "RC9",
            },
            {
                "LAchildID": "child3",
                "DateOfInitialCPC": pd.NA,  # 2 fail : found in Section47 and CPP tables
                "CINdetailsID": "cinID3",
                "ReasonForClosure": "RC8",
            },
            {
                "LAchildID": "child4",
                "DateOfInitialCPC": "28/05/2000",  # 3 fails for having initialcpc
                "CINdetailsID": "cinID4",
                "ReasonForClosure": "RC8",
            },
            {
                "LAchildID": "child3",
                "DateOfInitialCPC": "26/05/2000",
                "CINdetailsID": "cinID2",
                "ReasonForClosure": "RC10",  # 4 ignore: reason != RC8
            },
            {
                "LAchildID": "child3",
                "DateOfInitialCPC": "26/05/2003",
                "CINdetailsID": "cinID8",
                "ReasonForClosure": "RC10",  # 5 ignore: reason != RC8
            },
            {
                "LAchildID": "child3",
                "DateOfInitialCPC": "14/03/2001",
                "CINdetailsID": "cinID4",
                "ReasonForClosure": "RC10",  # 6 ignore: reason != RC8
            },
            {
                "LAchildID": "child7",
                "DateOfInitialCPC": pd.NA,
                "CINdetailsID": "cinID4",
                "ReasonForClosure": "RC10",  # 7 pass
            },
        ]
    )
    sample_cin_plan_dates = pd.DataFrame(
        [
            {
                "LAchildID": "child1",  # fails for having a module
                "CINdetailsID": "cinID1",
            },
            {
                "LAchildID": "child1",
                "CINdetailsID": "cinID2",
            },
            {
                "LAchildID": "child1",
                "CINdetailsID": "cinID3",
            },
            {
                "LAchildID": "child1",
                "CINdetailsID": "cinID4",
            },
        ]
    )

    sample_cin_details["DateOfInitialCPC"] = pd.to_datetime(
        sample_cin_details["DateOfInitialCPC"], format="%d/%m/%Y", errors="coerce"
    )
    sample_section47["DateOfInitialCPC"] = pd.to_datetime(
        sample_section47["DateOfInitialCPC"], format="%d/%m/%Y", errors="coerce"
    )

    result = run_rule(
        validate,
        {
            ChildProtectionPlans: sample_cpp,
            Section47: sample_section47,
            CINdetails: sample_cin_details,
            CINplanDates: sample_cin_plan_dates,
        },
    )

    issues_list = result.type2_issues
    assert len(issues_list) == 4
    issues = issues_list[1]

    issue_table = issues.table
    assert issue_table == CINdetails

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
                    "child1",  # ChildID
                    "cinID1",  # CINdetailsID
                ),
                "ROW_ID": [0],
            },
            {
                "ERROR_ID": (
                    "child2",  # ChildID
                    "cinID2",  # CINdetailsID
                ),
                "ROW_ID": [1],
            },
            {
                "ERROR_ID": (
                    "child3",  # ChildID
                    "cinID3",  # CINdetailsID
                ),
                "ROW_ID": [2],
            },
            {
                "ERROR_ID": (
                    "child4",  # ChildID
                    "cinID4",  # CINdetailsID
                ),
                "ROW_ID": [3],
            },
        ]
    )
    assert issue_rows.equals(expected_df)

    assert result.definition.code == "2990"
    assert (
        result.definition.message
        == "Activity is recorded against a case marked as ‘Case closed after assessment, no further action’ or 'case closed after assessment, referred to early help'."
    )
