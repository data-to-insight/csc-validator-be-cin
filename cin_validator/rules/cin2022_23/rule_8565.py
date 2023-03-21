"""
Rule number: 8565
Module: CIN Details
Rule details: If <CINclosureDate> (N00102) is present then it must be on or after all of the following dates that are present:
    <AssessmentActualStartDate> (N00159)
    <AssessmentAuthorisationDate>(N00160)
    <S47ActualStartDate> (N00148)
    <DateOfInitialCPC> (N00110)
    <CPPendDate> (N00115)
    <CINPlanStartDate> (N00689)
    <CINPlanEndDate> (N00690)
Rule message: Activity shown after a case has been closed

"""
from typing import Mapping

import pandas as pd

from cin_validator.rule_engine import (
    CINTable,
    IssueLocator,
    RuleContext,
    rule_definition,
)
from cin_validator.test_engine import run_rule

# Get tables and columns of interest from the CINTable object defined in rule_engine/__api.py

CINDetails = CINTable.CINdetails
LAchildID = CINDetails.LAchildID
CINdetailsID = CINDetails.CINdetailsID
CINclosureDate = CINDetails.CINclosureDate
DateOfInitialCPC = CINDetails.DateOfInitialCPC

Assessments = CINTable.Assessments
LAchildID = Assessments.LAchildID
CINdetailsID = Assessments.CINdetailsID
AssessmentActualStartDate = Assessments.AssessmentActualStartDate
AssessmentAuthorisationDate = Assessments.AssessmentAuthorisationDate

Section47 = CINTable.Section47
LAchildID = Section47.LAchildID
CINdetailsID = Section47.CINdetailsID
S47ActualStartDate = Section47.S47ActualStartDate

ChildProtectionPlans = CINTable.ChildProtectionPlans
LAchildID = ChildProtectionPlans.LAchildID
CINdetailsID = ChildProtectionPlans.CINdetailsID
CPPendDate = ChildProtectionPlans.CPPendDate

CINplanDates = CINTable.CINplanDates
LAchildID = CINplanDates.LAchildID
CINdetailsID = CINplanDates.CINdetailsID
CINPlanStartDate = CINplanDates.CINPlanStartDate
CINPlanEndDate = CINplanDates.CINPlanEndDate


@rule_definition(
    code=8565,
    module=CINTable.ChildProtectionPlans,
    message="Activity shown after a case has been closed",
    affected_fields=[
        CINclosureDate,
        DateOfInitialCPC,
        AssessmentActualStartDate,
        AssessmentAuthorisationDate,
        S47ActualStartDate,
        CPPendDate,
        CINPlanStartDate,
        CINPlanEndDate,
    ],
)
def validate(
    data_container: Mapping[CINTable, pd.DataFrame], rule_context: RuleContext
):
    df_CIN = data_container[CINDetails].copy()
    df_assessments = data_container[Assessments].copy()
    df_S47 = data_container[Section47].copy()
    df_CPP = data_container[ChildProtectionPlans].copy()
    df_CINplan = data_container[CINplanDates].copy()

    df_CIN.index.name = "ROW_ID"
    df_assessments.index.name = "ROW_ID"
    df_S47.index.name = "ROW_ID"
    df_CPP.index.name = "ROW_ID"
    df_CINplan.index.name = "ROW_ID"

    df_CIN.reset_index(inplace=True)
    df_assessments.reset_index(inplace=True)
    df_S47.reset_index(inplace=True)
    df_CPP.reset_index(inplace=True)
    df_CINplan.reset_index(inplace=True)

    # Rule details: If <CINclosureDate> (N00102) is present then it must be on or after all of the following dates that are present:
    #     <AssessmentActualStartDate> (N00159)
    #     <AssessmentAuthorisationDate>(N00160)
    #     <S47ActualStartDate> (N00148)
    #     <DateOfInitialCPC> (N00110)
    #     <CPPendDate> (N00115)
    #     <CINPlanStartDate> (N00689)
    #     <CINPlanEndDate> (N00690)

    # Remove rows without a CIN closure date
    df_CIN = df_CIN[df_CIN[CINclosureDate].notna()]

    # <CINclosureDate> (N00102) is present then it must be on or after all of the following dates that are present:
    # <AssessmentActualStartDate> (N00159)
    # <AssessmentAuthorisationDate>(N00160)
    # <S47ActualStartDate> (N00148)
    # <DateOfInitialCPC> (N00110)
    # <CPPendDate> (N00115)
    # <CINPlanStartDate> (N00689)
    # <CINPlanEndDate> (N00690)

    # Join tables together
    df_CIN_assessments = df_CIN.copy().merge(
        df_assessments.copy(),
        on=[LAchildID, CINdetailsID],
        how="left",
        suffixes=["_CIN", "_assessments"],
    )

    df_CIN_S47 = df_CIN.copy().merge(
        df_S47.copy(),
        on=[LAchildID, CINdetailsID],
        how="left",
        suffixes=["_CIN", "_S47"],
    )

    df_CIN_CPP = df_CIN.copy().merge(
        df_CPP.copy(),
        on=[LAchildID, CINdetailsID],
        how="left",
        suffixes=["_CIN", "_CPP"],
    )

    df_CIN_CINPlan = df_CIN.copy().merge(
        df_CINplan.copy(),
        on=[LAchildID, CINdetailsID],
        how="left",
        suffixes=["_CIN", "_CINPlan"],
    )

    df = (
        df_CIN_assessments.merge(
            df_CIN_S47,
            left_on=[
                LAchildID,
                CINdetailsID,
                "ROW_ID_CIN",
                CINclosureDate,
                "DateOfInitialCPC",
            ],
            right_on=[
                LAchildID,
                CINdetailsID,
                "ROW_ID_CIN",
                CINclosureDate,
                "DateOfInitialCPC_CIN",  # Merges on DateOfInitialCPC from the CIN module
            ],
            suffixes=("_dd", "_done"),
        )
        .merge(
            df_CIN_CPP,
            on=[
                LAchildID,
                CINdetailsID,
                "ROW_ID_CIN",
                CINclosureDate,
                DateOfInitialCPC,
            ],
        )
        .merge(
            df_CIN_CINPlan,
            on=[
                LAchildID,
                CINdetailsID,
                "ROW_ID_CIN",
                CINclosureDate,
                DateOfInitialCPC,
            ],
        )
    )

    # Return those where dates don't align
    # From the merges, DateOfInitialCPC IS DateOfInitialCPC_CIN, we end up with two of the same column, one with a suffix and one without because CIN
    # Merges with tables both with and without DateOfInitalCPC fields. This condition is set to check DateOfInitialCPC_CIN for clarity in what it's doing.
    condition1 = df[CINclosureDate] < df["DateOfInitialCPC_CIN"]
    condition2 = df[CINclosureDate] < df[AssessmentActualStartDate]
    condition3 = df[CINclosureDate] < df[AssessmentAuthorisationDate]
    condition4 = df[CINclosureDate] < df[S47ActualStartDate]
    condition5 = df[CINclosureDate] < df[CPPendDate]
    condition6 = df[CINclosureDate] < df[CINPlanStartDate]
    condition7 = df[CINclosureDate] < df[CINPlanEndDate]
    condition8 = (
        df[CINclosureDate] < df["DateOfInitialCPC_S47"]
    )  # & (df["DateOfInitialCPC_CIN"].notna())

    df = df[
        condition1
        | condition2
        | condition3
        | condition4
        | condition5
        | condition6
        | condition7
        | condition8
    ].reset_index()

    df["ERROR_ID"] = tuple(
        zip(
            df[LAchildID],
            df[CINdetailsID],
            df[CINclosureDate],
            df["DateOfInitialCPC_CIN"],
            df[AssessmentActualStartDate],
            df[AssessmentAuthorisationDate],
            df[S47ActualStartDate],
            df[CPPendDate],
            df[CINPlanStartDate],
            df[CINPlanEndDate],
            df["DateOfInitialCPC_S47"],
        )
    )

    df_CIN_issues = (
        df_CIN.merge(df, left_on="ROW_ID", right_on="ROW_ID_CIN")
        .groupby("ERROR_ID")["ROW_ID"]
        .apply(list)
        .reset_index()
    )

    df_assessments_isses = (
        df_assessments.merge(df, left_on="ROW_ID", right_on="ROW_ID_assessments")
        .groupby("ERROR_ID")["ROW_ID"]
        .apply(list)
        .reset_index()
    )

    df_S47_issues = (
        df_S47.merge(df, left_on="ROW_ID", right_on="ROW_ID_S47")
        .groupby("ERROR_ID")["ROW_ID"]
        .apply(list)
        .reset_index()
    )

    df_CPP_issues = (
        df_CPP.merge(df, left_on="ROW_ID", right_on="ROW_ID_CPP")
        .groupby("ERROR_ID")["ROW_ID"]
        .apply(list)
        .reset_index()
    )

    df_CINplan_issues = (
        df_CINplan.merge(df, left_on="ROW_ID", right_on="ROW_ID_CINPlan")
        .groupby("ERROR_ID")["ROW_ID"]
        .apply(list)
        .reset_index()
    )
    rule_context.push_type_2(
        table=CINDetails,
        columns=[CINclosureDate, DateOfInitialCPC],
        row_df=df_CIN_issues,
    )
    rule_context.push_type_2(
        table=Assessments,
        columns=[AssessmentActualStartDate, AssessmentAuthorisationDate],
        row_df=df_assessments_isses,
    )
    rule_context.push_type_2(
        table=Section47,
        columns=[S47ActualStartDate, DateOfInitialCPC],
        row_df=df_S47_issues,
    )
    rule_context.push_type_2(
        table=ChildProtectionPlans, columns=[CPPendDate], row_df=df_CPP_issues
    )
    rule_context.push_type_2(
        table=CINplanDates,
        columns=[CINPlanStartDate, CINPlanEndDate],
        row_df=df_CINplan_issues,
    )


def test_validate():
    sample_CIN = pd.DataFrame(
        [
            {
                "LAchildID": "child1",
                "CINdetailsID": "cinID1",
                "CINclosureDate": "01/01/2022",
                "DateOfInitialCPC": "30/12/2020"
                # Pass
            },
            {
                "LAchildID": "child2",
                "CINdetailsID": "cinID2",
                "CINclosureDate": "01/01/2022",
                "DateOfInitialCPC": "30/12/2022"  # Initial CPC after CIN closure date
                # Fail
            },
            {
                "LAchildID": "child3",
                "CINdetailsID": "cinID3",
                "CINclosureDate": "01/01/2022",
                "DateOfInitialCPC": "30/12/2020"
                # Fails on assessment start date
            },
            {
                "LAchildID": "child4",
                "CINdetailsID": "cinID4",
                "CINclosureDate": "01/01/2022",
                "DateOfInitialCPC": "30/12/2020"
                # Fails on assessment authorisation date
            },
            {
                "LAchildID": "child5",
                "CINdetailsID": "cinID5",
                "CINclosureDate": "01/01/2022",
                "DateOfInitialCPC": "30/12/2020"
                # Fails on S47 date
            },
            {
                "LAchildID": "child6",
                "CINdetailsID": "cinID6",
                "CINclosureDate": "01/01/2022",
                "DateOfInitialCPC": "30/12/2020"
                # Fails on CPP start date
            },
            {
                "LAchildID": "child7",
                "CINdetailsID": "cinID7",
                "CINclosureDate": "01/01/2022",
                "DateOfInitialCPC": "30/12/2020"
                # Fails on CIN plan start date
            },
            {
                "LAchildID": "child8",
                "CINdetailsID": "cinID8",
                "CINclosureDate": "01/01/2022",
                "DateOfInitialCPC": "30/12/2020"
                # Fails on CIN plan end date
            },
            {
                "LAchildID": "child10",
                "CINdetailsID": "cinID1",
                "CINclosureDate": "01/09/2021",
                "DateOfInitialCPC": pd.NA
                # Fail on s47 DOICPC
            },
        ]
    )

    sample_CIN["CINclosureDate"] = pd.to_datetime(
        sample_CIN["CINclosureDate"], format="%d/%m/%Y", errors="coerce"
    )
    sample_CIN["DateOfInitialCPC"] = pd.to_datetime(
        sample_CIN["DateOfInitialCPC"], format="%d/%m/%Y", errors="coerce"
    )

    sample_assessments = pd.DataFrame(
        [
            {
                "LAchildID": "child1",
                "CINdetailsID": "cinID1",
                "AssessmentActualStartDate": "01/01/2021",
                "AssessmentAuthorisationDate": "30/12/2020"
                # Pass
            },
            {
                "LAchildID": "child2",
                "CINdetailsID": "cinID2",
                "AssessmentActualStartDate": "01/01/2021",
                "AssessmentAuthorisationDate": "30/05/2020"
                # Fails on initial CPC date
            },
            {
                "LAchildID": "child3",
                "CINdetailsID": "cinID3",
                "AssessmentActualStartDate": "31/01/2022",  # Fail, assessment start after CIN closure
                "AssessmentAuthorisationDate": "30/05/2020"
                # Fail
            },
            {
                "LAchildID": "child4",
                "CINdetailsID": "cinID4",
                "AssessmentActualStartDate": "01/01/2021",
                "AssessmentAuthorisationDate": "30/05/2022"  # Fail, assesment authorised after CIN closure
                # Fail
            },
            {
                "LAchildID": "child5",
                "CINdetailsID": "cinID5",
                "AssessmentActualStartDate": "01/01/2021",
                "AssessmentAuthorisationDate": "30/05/2020"
                # Fails on S47 date
            },
            {
                "LAchildID": "child6",
                "CINdetailsID": "cinID6",
                "AssessmentActualStartDate": "01/01/2021",
                "AssessmentAuthorisationDate": "30/05/2020"
                # Fails on CPP start date
            },
            {
                "LAchildID": "child7",
                "CINdetailsID": "cinID7",
                "AssessmentActualStartDate": "01/01/2021",
                "AssessmentAuthorisationDate": "30/05/2020"
                # Fails on CIN plan start date
            },
            {
                "LAchildID": "child8",
                "CINdetailsID": "cinID8",
                "AssessmentActualStartDate": "01/01/2021",
                "AssessmentAuthorisationDate": "30/05/2020"
                # Fails on CIN plan end date
            },
            {
                "LAchildID": "child10",
                "CINdetailsID": "cinID1",
                "AssessmentActualStartDate": "01/07/2021",
                "AssessmentAuthorisationDate": "01/09/2021",
            },
        ]
    )

    sample_assessments["AssessmentActualStartDate"] = pd.to_datetime(
        sample_assessments["AssessmentActualStartDate"],
        format="%d/%m/%Y",
        errors="coerce",
    )
    sample_assessments["AssessmentAuthorisationDate"] = pd.to_datetime(
        sample_assessments["AssessmentAuthorisationDate"],
        format="%d/%m/%Y",
        errors="coerce",
    )

    sample_S47 = pd.DataFrame(
        [
            {
                "LAchildID": "child1",
                "CINdetailsID": "cinID1",
                "S47ActualStartDate": "01/01/2021",
                "DateOfInitialCPC": "30/12/2020"
                # Pass
            },
            {
                "LAchildID": "child2",
                "CINdetailsID": "cinID2",
                "S47ActualStartDate": "01/01/2021",
                "DateOfInitialCPC": "30/12/2020"
                # Fails on initial CPC date
            },
            {
                "LAchildID": "child3",
                "CINdetailsID": "cinID3",
                "S47ActualStartDate": "01/01/2021",
                "DateOfInitialCPC": "30/12/2020"
                # Fails on assessment start date
            },
            {
                "LAchildID": "child4",
                "CINdetailsID": "cinID4",
                "S47ActualStartDate": "01/01/2021",
                "DateOfInitialCPC": "30/12/2020"
                # Fails on assessment authorisation date
            },
            {
                "LAchildID": "child5",
                "CINdetailsID": "cinID5",
                "S47ActualStartDate": "31/07/2022",
                "DateOfInitialCPC": "30/12/2020"  # Fails S47 starts after CIN closure
                # Fail
            },
            {
                "LAchildID": "child6",
                "CINdetailsID": "cinID6",
                "S47ActualStartDate": "01/01/2021",
                "DateOfInitialCPC": "30/12/2020"
                # Fails on CPP start date
            },
            {
                "LAchildID": "child7",
                "CINdetailsID": "cinID7",
                "S47ActualStartDate": "01/01/2021",
                "DateOfInitialCPC": "30/12/2020"
                # Fails on CIN plan start date
            },
            {
                "LAchildID": "child8",
                "CINdetailsID": "cinID8",
                "S47ActualStartDate": "01/01/2021",
                "DateOfInitialCPC": "30/12/2020"
                # Fails on CIN plan end date
            },
            {
                "LAchildID": "child10",
                "CINdetailsID": "cinID1",
                "S47ActualStartDate": "01/07/2021",
                "DateOfInitialCPC": "01/10/2022",
                # Fails on CIN plan end date
            },
        ]
    )

    sample_S47["S47ActualStartDate"] = pd.to_datetime(
        sample_S47["S47ActualStartDate"], format="%d/%m/%Y", errors="coerce"
    )
    sample_S47["DateOfInitialCPC"] = pd.to_datetime(
        sample_S47["DateOfInitialCPC"], format="%d/%m/%Y", errors="coerce"
    )

    sample_CPP = pd.DataFrame(
        [
            {
                "LAchildID": "child1",
                "CINdetailsID": "cinID1",
                "CPPendDate": "30/12/2020"
                # Pass
            },
            {
                "LAchildID": "child2",
                "CINdetailsID": "cinID2",
                "CPPendDate": "30/12/2020"
                # Fails on initial CPC date
            },
            {
                "LAchildID": "child3",
                "CINdetailsID": "cinID3",
                "CPPendDate": "30/12/2020"
                # Fails on assessment start date
            },
            {
                "LAchildID": "child4",
                "CINdetailsID": "cinID4",
                "CPPendDate": "30/12/2020"
                # Fails on assessment authorisation date
            },
            {
                "LAchildID": "child5",
                "CINdetailsID": "cinID5",
                "CPPendDate": "30/12/2020"
                # Fails on S47 date
            },
            {
                "LAchildID": "child6",
                "CINdetailsID": "cinID6",
                "CPPendDate": "30/12/2022"  # Fail, CPP start date after CIN closure
                # Fail
            },
            {
                "LAchildID": "child7",
                "CINdetailsID": "cinID7",
                "CPPendDate": "30/12/2020"
                # Fails on CIN plan start date
            },
            {
                "LAchildID": "child8",
                "CINdetailsID": "cinID8",
                "CPPendDate": "30/12/2020"
                # Fails on CIN plan end date
            },
        ]
    )

    sample_CPP["CPPendDate"] = pd.to_datetime(
        sample_CPP["CPPendDate"], format="%d/%m/%Y", errors="coerce"
    )

    sample_CINplan = pd.DataFrame(
        [
            {
                "LAchildID": "child1",
                "CINdetailsID": "cinID1",
                "CINPlanStartDate": "01/01/2020",
                "CINPlanEndDate": "30/12/2020"
                # Pass
            },
            {
                "LAchildID": "child2",
                "CINdetailsID": "cinID2",
                "CINPlanStartDate": "01/01/2020",
                "CINPlanEndDate": "30/12/2020"
                # Fails on initial CPC date
            },
            {
                "LAchildID": "child3",
                "CINdetailsID": "cinID3",
                "CINPlanStartDate": "01/01/2020",
                "CINPlanEndDate": "30/12/2020"
                # Fails on assessment start date
            },
            {
                "LAchildID": "child4",
                "CINdetailsID": "cinID4",
                "CINPlanStartDate": "01/01/2020",
                "CINPlanEndDate": "30/12/2020"
                # Fails on assessment authorisation date
            },
            {
                "LAchildID": "child5",
                "CINdetailsID": "cinID5",
                "CINPlanStartDate": "01/01/2020",
                "CINPlanEndDate": "30/12/2020"
                # Fails on S47 date
            },
            {
                "LAchildID": "child6",
                "CINdetailsID": "cinID6",
                "CINPlanStartDate": "01/01/2020",
                "CINPlanEndDate": "30/12/2020"
                # Fails on CPP start date
            },
            {
                "LAchildID": "child7",
                "CINdetailsID": "cinID7",
                "CINPlanStartDate": "01/06/2022",  # Fail, CIN plan starts after CIN closure
                "CINPlanEndDate": "30/12/2020"
                # Fail
            },
            {
                "LAchildID": "child8",
                "CINdetailsID": "cinID8",
                "CINPlanStartDate": "01/01/2020",
                "CINPlanEndDate": "30/12/2022"  # Fail, CIN plan ends after CIN closure
                # Fail
            },
        ]
    )

    sample_CINplan["CINPlanStartDate"] = pd.to_datetime(
        sample_CINplan["CINPlanStartDate"], format="%d/%m/%Y", errors="coerce"
    )
    sample_CINplan["CINPlanEndDate"] = pd.to_datetime(
        sample_CINplan["CINPlanEndDate"], format="%d/%m/%Y", errors="coerce"
    )

    result = run_rule(
        validate,
        {
            CINDetails: sample_CIN,
            Assessments: sample_assessments,
            Section47: sample_S47,
            ChildProtectionPlans: sample_CPP,
            CINplanDates: sample_CINplan,
        },
    )

    issues_list = result.type2_issues
    assert len(issues_list) == 5

    issues = issues_list[0]

    issue_table = issues.table
    assert issue_table == CINDetails

    issue_columns = issues.columns
    assert issue_columns == [
        CINclosureDate,
        DateOfInitialCPC,
    ]

    issue_rows = issues.row_df

    assert len(issue_rows) == 8
    assert isinstance(issue_rows, pd.DataFrame)
    assert issue_rows.columns.to_list() == ["ERROR_ID", "ROW_ID"]

    expected_df = pd.DataFrame(
        [
            {
                "ERROR_ID": (
                    "child10",  # ChildID
                    # CIN ID
                    "cinID1",
                    # CIN closure date
                    pd.to_datetime("01/09/2021", format="%d/%m/%Y", errors="coerce"),
                    # Initial CPC date
                    pd.NaT,
                    # Assessment start date
                    pd.to_datetime("01/07/2021", format="%d/%m/%Y", errors="coerce"),
                    # Assessment authorisation date
                    pd.to_datetime("01/09/2021", format="%d/%m/%Y", errors="coerce"),
                    # S47 start date
                    pd.to_datetime("01/07/2021", format="%d/%m/%Y", errors="coerce"),
                    # CPP start date
                    pd.NaT,
                    # CIN start date
                    pd.NaT,
                    # CIN end date
                    pd.NaT,
                    # S47 DateOfInitialCPC
                    pd.to_datetime("01/10/2022", format="%d/%m/%Y", errors="coerce"),
                ),
                "ROW_ID": [8],
            },
            {
                "ERROR_ID": (
                    # ChildID
                    "child2",
                    # CIN ID
                    "cinID2",
                    # CIN closure date
                    pd.to_datetime("01/01/2022", format="%d/%m/%Y", errors="coerce"),
                    # Initial CPC date
                    pd.to_datetime("30/12/2022", format="%d/%m/%Y", errors="coerce"),
                    # Assessment start date
                    pd.to_datetime("01/01/2021", format="%d/%m/%Y", errors="coerce"),
                    # Assessment authorisation date
                    pd.to_datetime("30/05/2020", format="%d/%m/%Y", errors="coerce"),
                    # S47 start date
                    pd.to_datetime("01/01/2021", format="%d/%m/%Y", errors="coerce"),
                    # CPP start date
                    pd.to_datetime("30/12/2020", format="%d/%m/%Y", errors="coerce"),
                    # CIN start date
                    pd.to_datetime("01/01/2020", format="%d/%m/%Y", errors="coerce"),
                    # CIN end date
                    pd.to_datetime("30/12/2020", format="%d/%m/%Y", errors="coerce"),
                    # S47 DateOfInitialCPC
                    pd.to_datetime("30/12/2020", format="%d/%m/%Y", errors="coerce"),
                ),
                "ROW_ID": [1],
            },
            {
                "ERROR_ID": (
                    "child3",  # ChildID
                    # CIN ID
                    "cinID3",
                    # CIN closure date
                    pd.to_datetime("01/01/2022", format="%d/%m/%Y", errors="coerce"),
                    # Initial CPC date
                    pd.to_datetime("30/12/2020", format="%d/%m/%Y", errors="coerce"),
                    # Assessment start date
                    pd.to_datetime("31/01/2022", format="%d/%m/%Y", errors="coerce"),
                    # Assessment authorisation date
                    pd.to_datetime("30/05/2020", format="%d/%m/%Y", errors="coerce"),
                    # S47 start date
                    pd.to_datetime("01/01/2021", format="%d/%m/%Y", errors="coerce"),
                    # CPP start date
                    pd.to_datetime("30/12/2020", format="%d/%m/%Y", errors="coerce"),
                    # CIN start date
                    pd.to_datetime("01/01/2020", format="%d/%m/%Y", errors="coerce"),
                    # CIN end date
                    pd.to_datetime("30/12/2020", format="%d/%m/%Y", errors="coerce"),
                    # S47 DateOfInitialCPC
                    pd.to_datetime("30/12/2020", format="%d/%m/%Y", errors="coerce"),
                ),
                "ROW_ID": [2],
            },
            {
                "ERROR_ID": (
                    "child4",  # ChildID
                    # CIN ID
                    "cinID4",
                    # CIN closure date
                    pd.to_datetime("01/01/2022", format="%d/%m/%Y", errors="coerce"),
                    # Initial CPC date
                    pd.to_datetime("30/12/2020", format="%d/%m/%Y", errors="coerce"),
                    # Assessment start date
                    pd.to_datetime("01/01/2021", format="%d/%m/%Y", errors="coerce"),
                    # Assessment authorisation date
                    pd.to_datetime("30/05/2022", format="%d/%m/%Y", errors="coerce"),
                    # S47 start date
                    pd.to_datetime("01/01/2021", format="%d/%m/%Y", errors="coerce"),
                    # CPP start date
                    pd.to_datetime("30/12/2020", format="%d/%m/%Y", errors="coerce"),
                    # CIN start date
                    pd.to_datetime("01/01/2020", format="%d/%m/%Y", errors="coerce"),
                    # CIN end date
                    pd.to_datetime("30/12/2020", format="%d/%m/%Y", errors="coerce"),
                    # S47 DateOfInitialCPC
                    pd.to_datetime("30/12/2020", format="%d/%m/%Y", errors="coerce"),
                ),
                "ROW_ID": [3],
            },
            {
                "ERROR_ID": (
                    "child5",  # ChildID
                    # CIN ID
                    "cinID5",
                    # CIN closure date
                    pd.to_datetime("01/01/2022", format="%d/%m/%Y", errors="coerce"),
                    # Initial CPC date
                    pd.to_datetime("30/12/2020", format="%d/%m/%Y", errors="coerce"),
                    # Assessment start date
                    pd.to_datetime("01/01/2021", format="%d/%m/%Y", errors="coerce"),
                    # Assessment authorisation date
                    pd.to_datetime("30/05/2020", format="%d/%m/%Y", errors="coerce"),
                    # S47 start date
                    pd.to_datetime("31/07/2022", format="%d/%m/%Y", errors="coerce"),
                    # CPP start date
                    pd.to_datetime("30/12/2020", format="%d/%m/%Y", errors="coerce"),
                    # CIN start date
                    pd.to_datetime("01/01/2020", format="%d/%m/%Y", errors="coerce"),
                    # CIN end date
                    pd.to_datetime("30/12/2020", format="%d/%m/%Y", errors="coerce"),
                    # S47 DateOfInitialCPC
                    pd.to_datetime("30/12/2020", format="%d/%m/%Y", errors="coerce"),
                ),
                "ROW_ID": [4],
            },
            {
                "ERROR_ID": (
                    "child6",  # ChildID
                    # CIN ID
                    "cinID6",
                    # CIN closure date
                    pd.to_datetime("01/01/2022", format="%d/%m/%Y", errors="coerce"),
                    # Initial CPC date
                    pd.to_datetime("30/12/2020", format="%d/%m/%Y", errors="coerce"),
                    # Assessment start date
                    pd.to_datetime("01/01/2021", format="%d/%m/%Y", errors="coerce"),
                    # Assessment authorisation date
                    pd.to_datetime("30/05/2020", format="%d/%m/%Y", errors="coerce"),
                    # S47 start date
                    pd.to_datetime("01/01/2021", format="%d/%m/%Y", errors="coerce"),
                    # CPP start date
                    pd.to_datetime("30/12/2022", format="%d/%m/%Y", errors="coerce"),
                    # CIN start date
                    pd.to_datetime("01/01/2020", format="%d/%m/%Y", errors="coerce"),
                    # CIN end date
                    pd.to_datetime("30/12/2020", format="%d/%m/%Y", errors="coerce"),
                    # S47 DateOfInitialCPC
                    pd.to_datetime("30/12/2020", format="%d/%m/%Y", errors="coerce"),
                ),
                "ROW_ID": [5],
            },
            {
                "ERROR_ID": (
                    "child7",  # ChildID
                    # CIN ID
                    "cinID7",
                    # CIN closure date
                    pd.to_datetime("01/01/2022", format="%d/%m/%Y", errors="coerce"),
                    # Initial CPC date
                    pd.to_datetime("30/12/2020", format="%d/%m/%Y", errors="coerce"),
                    # Assessment start date
                    pd.to_datetime("01/01/2021", format="%d/%m/%Y", errors="coerce"),
                    # Assessment authorisation date
                    pd.to_datetime("30/05/2020", format="%d/%m/%Y", errors="coerce"),
                    # S47 start date
                    pd.to_datetime("01/01/2021", format="%d/%m/%Y", errors="coerce"),
                    # CPP start date
                    pd.to_datetime("30/12/2020", format="%d/%m/%Y", errors="coerce"),
                    # CIN start date
                    pd.to_datetime("01/06/2022", format="%d/%m/%Y", errors="coerce"),
                    # CIN end date
                    pd.to_datetime("30/12/2020", format="%d/%m/%Y", errors="coerce"),
                    # S47 DateOfInitialCPC
                    pd.to_datetime("30/12/2020", format="%d/%m/%Y", errors="coerce"),
                ),
                "ROW_ID": [6],
            },
            {
                "ERROR_ID": (
                    "child8",  # ChildID
                    # CIN ID
                    "cinID8",
                    # CIN closure date
                    pd.to_datetime("01/01/2022", format="%d/%m/%Y", errors="coerce"),
                    # Initial CPC date
                    pd.to_datetime("30/12/2020", format="%d/%m/%Y", errors="coerce"),
                    # Assessment start date
                    pd.to_datetime("01/01/2021", format="%d/%m/%Y", errors="coerce"),
                    # Assessment authorisation date
                    pd.to_datetime("30/05/2020", format="%d/%m/%Y", errors="coerce"),
                    # S47 start date
                    pd.to_datetime("01/01/2021", format="%d/%m/%Y", errors="coerce"),
                    # CPP start date
                    pd.to_datetime("30/12/2020", format="%d/%m/%Y", errors="coerce"),
                    # CIN start date
                    pd.to_datetime("01/01/2020", format="%d/%m/%Y", errors="coerce"),
                    # CIN end date
                    pd.to_datetime("30/12/2022", format="%d/%m/%Y", errors="coerce"),
                    # S47 DateOfInitialCPC
                    pd.to_datetime("30/12/2020", format="%d/%m/%Y", errors="coerce"),
                ),
                "ROW_ID": [7],
            },
        ]
    )

    assert issue_rows.equals(expected_df)

    assert result.definition.code == 8565
    assert result.definition.message == "Activity shown after a case has been closed"
