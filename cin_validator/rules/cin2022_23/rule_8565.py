"""
Rule number: '8565'
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

from cin_validator.rule_engine import CINTable, RuleContext, rule_definition
from cin_validator.test_engine import run_rule

# Get tables and columns of interest from the CINTable object defined in rule_engine/__api.py

CINdetails = CINTable.CINdetails
LAchildID = CINdetails.LAchildID
CINdetailsID = CINdetails.CINdetailsID
CINclosureDate = CINdetails.CINclosureDate
DateOfInitialCPC = CINdetails.DateOfInitialCPC

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
    code="8565",
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
    df_cin = data_container[CINdetails]
    df_ass = data_container[Assessments]
    df_47 = data_container[Section47]
    df_cpp = data_container[ChildProtectionPlans]
    df_plan = data_container[CINplanDates]

    df_cin.index.name = "ROW_ID"
    df_ass.index.name = "ROW_ID"
    df_47.index.name = "ROW_ID"
    df_cpp.index.name = "ROW_ID"
    df_plan.index.name = "ROW_ID"

    df_cin.reset_index(inplace=True)
    df_ass.reset_index(inplace=True)
    df_47.reset_index(inplace=True)
    df_cpp.reset_index(inplace=True)
    df_plan.reset_index(inplace=True)

    # Rule details: If <CINclosureDate> (N00102) is present then it must be on or after all of the following dates that are present:
    #     <AssessmentActualStartDate> (N00159)
    #     <AssessmentAuthorisationDate>(N00160)
    #     <S47ActualStartDate> (N00148)
    #     <DateOfInitialCPC> (N00110)
    #     <CPPendDate> (N00115)
    #     <CINPlanStartDate> (N00689)
    #     <CINPlanEndDate> (N00690)

    # Remove rows without a CIN closure date
    df_cin = df_cin[df_cin[CINclosureDate].notna()]

    # CIN TABLE
    df_cin["CINclosureDate"] = pd.to_datetime(
        df_cin["CINclosureDate"], format="%d/%m/%Y", errors="coerce"
    )
    df_cin["DateOfInitialCPC"] = pd.to_datetime(
        df_cin["DateOfInitialCPC"], format="%d/%m/%Y", errors="coerce"
    )

    df_cin_fail = df_cin[df_cin["CINclosureDate"] < df_cin["DateOfInitialCPC"]]
    df_cin_fail["ERROR_ID"] = tuple(
        zip(
            df_cin_fail["LAchildID"],
            df_cin_fail["CINdetailsID"],
            df_cin_fail["CINclosureDate"],
        )
    )

    df_cin_issues_cin = (
        df_cin_fail.groupby("ERROR_ID", group_keys=False)["ROW_ID"]
        .apply(list)
        .reset_index()
    )

    # ASSESSMENTS TABLE
    df_cin_ass = df_cin.merge(
        df_ass,
        on=["LAchildID", "CINdetailsID"],
        how="left",
        suffixes=["_cin", "_ass"],
    )

    df_cin_ass_fail = df_cin_ass[
        (df_cin_ass["CINclosureDate"] < df_cin_ass["AssessmentActualStartDate"])
        | (df_cin_ass["CINclosureDate"] < df_cin_ass["AssessmentAuthorisationDate"])
    ]
    df_cin_ass_fail["ERROR_ID"] = tuple(
        zip(
            df_cin_ass_fail["LAchildID"],
            df_cin_ass_fail["CINdetailsID"],
            df_cin_ass_fail["CINclosureDate"],
        )
    )

    df_cin_issues_ass = (
        df_cin.merge(df_cin_ass_fail, left_on="ROW_ID", right_on="ROW_ID_cin")
        .groupby("ERROR_ID", group_keys=False)["ROW_ID"]
        .apply(list)
        .reset_index()
    )
    df_ass_issues = (
        df_ass.merge(df_cin_ass_fail, left_on="ROW_ID", right_on="ROW_ID_ass")
        .groupby("ERROR_ID", group_keys=False)["ROW_ID"]
        .apply(list)
        .reset_index()
    )

    # SECTION47 TABLE
    df_cin_47 = df_cin.merge(
        df_47,
        on=["LAchildID", "CINdetailsID"],
        how="left",
        suffixes=["_cin", "_47"],
    )  # both have the DateOfInitialCPC column so suffixes are applied.

    df_cin_47_fail = df_cin_47[
        (df_cin_47["CINclosureDate"] < df_cin_47["S47ActualStartDate"])
        | (df_cin_47["CINclosureDate"] < df_cin_47["DateOfInitialCPC_47"])
    ]
    df_cin_47_fail["ERROR_ID"] = tuple(
        zip(
            df_cin_47_fail["LAchildID"],
            df_cin_47_fail["CINdetailsID"],
            df_cin_47_fail["CINclosureDate"],
        )
    )

    df_cin_issues_47 = (
        df_cin.merge(df_cin_47_fail, left_on="ROW_ID", right_on="ROW_ID_cin")
        .groupby("ERROR_ID", group_keys=False)["ROW_ID"]
        .apply(list)
        .reset_index()
    )
    df_47_issues = (
        df_47.merge(df_cin_47_fail, left_on="ROW_ID", right_on="ROW_ID_47")
        .groupby("ERROR_ID", group_keys=False)["ROW_ID"]
        .apply(list)
        .reset_index()
    )

    # CHILDPROTECTIONPLANS TABLE

    df_cpp = pd.DataFrame(
        [
            {
                "LAchildID": "child1",
                "CINdetailsID": "CINID1",
                "CPPendDate": "27/05/2022",  # fail
            }
        ]
    )

    df_cpp.index.name = "ROW_ID"
    df_cpp.reset_index(inplace=True)

    df_cpp["CPPendDate"] = pd.to_datetime(
        df_cpp["CPPendDate"], format="%d/%m/%Y", errors="coerce"
    )

    df_cin_cpp = df_cin.merge(
        df_cpp,
        on=["LAchildID", "CINdetailsID"],
        how="left",
        suffixes=["_cin", "_cpp"],
    )

    df_cin_cpp_fail = df_cin_cpp[
        df_cin_cpp["CINclosureDate"] < df_cin_cpp["CPPendDate"]
    ]
    df_cin_cpp_fail["ERROR_ID"] = tuple(
        zip(
            df_cin_cpp_fail["LAchildID"],
            df_cin_cpp_fail["CINdetailsID"],
            df_cin_cpp_fail["CINclosureDate"],
        )
    )

    df_cin_issues_cpp = (
        df_cin.merge(df_cin_cpp_fail, left_on="ROW_ID", right_on="ROW_ID_cin")
        .groupby("ERROR_ID", group_keys=False)["ROW_ID"]
        .apply(list)
        .reset_index()
    )
    df_cpp_issues = (
        df_cpp.merge(df_cin_cpp_fail, left_on="ROW_ID", right_on="ROW_ID_cpp")
        .groupby("ERROR_ID", group_keys=False)["ROW_ID"]
        .apply(list)
        .reset_index()
    )
    df_cpp_issues

    # CINPLANDATES TABLE
    df_cin_plan = df_cin.merge(
        df_plan,
        on=["LAchildID", "CINdetailsID"],
        how="left",
        suffixes=["_cin", "_plan"],
    )

    df_cin_plan_fail = df_cin_plan[
        (df_cin_plan["CINclosureDate"] < df_cin_plan["CINPlanStartDate"])
        | (df_cin_plan["CINclosureDate"] < df_cin_plan["CINPlanEndDate"])
    ]
    df_cin_plan_fail["ERROR_ID"] = tuple(
        zip(
            df_cin_47_fail["LAchildID"],
            df_cin_47_fail["CINdetailsID"],
            df_cin_47_fail["CINclosureDate"],
        )
    )

    df_cin_issues_plan = (
        df_cin.merge(df_cin_plan_fail, left_on="ROW_ID", right_on="ROW_ID_cin")
        .groupby("ERROR_ID", group_keys=False)["ROW_ID"]
        .apply(list)
        .reset_index()
    )
    df_plan_issues = (
        df_plan.merge(df_cin_plan_fail, left_on="ROW_ID", right_on="ROW_ID_plan")
        .groupby("ERROR_ID", group_keys=False)["ROW_ID"]
        .apply(list)
        .reset_index()
    )

    # combine failing cin locations from all tables
    cin_issues_all = pd.concat(
        [
            df_cin_issues_plan,
            df_cin_issues_cpp,
            df_cin_issues_47,
            df_cin_issues_ass,
            df_cin_issues_cin,
        ],
        ignore_index=True,
    )
    unique_cin_issues = cin_issues_all.astype(str).drop_duplicates().index
    df_cin_issues = cin_issues_all.loc[unique_cin_issues].reset_index(drop=True)

    rule_context.push_type_2(
        table=CINdetails,
        columns=[CINclosureDate, DateOfInitialCPC],
        row_df=df_cin_issues,
    )
    rule_context.push_type_2(
        table=Assessments,
        columns=[AssessmentActualStartDate, AssessmentAuthorisationDate],
        row_df=df_ass_issues,
    )
    rule_context.push_type_2(
        table=Section47,
        columns=[S47ActualStartDate, DateOfInitialCPC],
        row_df=df_47_issues,
    )
    rule_context.push_type_2(
        table=ChildProtectionPlans, columns=[CPPendDate], row_df=df_cpp_issues
    )
    rule_context.push_type_2(
        table=CINplanDates,
        columns=[CINPlanStartDate, CINPlanEndDate],
        row_df=df_plan_issues,
    )


def test_validate():
    df_cin = pd.DataFrame(
        [
            # Same CINclosureDate value is maintained throughout table for simplicity when cross-checking with other tables.
            {
                "LAchildID": "child1",
                "CINdetailsID": "CINID1",
                "CINclosureDate": "26/05/2022",
                "DateOfInitialCPC": "26/05/2022",  # pass
            },
            {
                "LAchildID": "child1",
                "CINdetailsID": "CINID2",
                "CINclosureDate": "26/05/2022",
                "DateOfInitialCPC": pd.NA,  # ignore. date is absent
            },
            {
                "LAchildID": "child1",
                "CINdetailsID": "CINID3",
                "CINclosureDate": "26/05/2022",
                "DateOfInitialCPC": "27/05/2022",  # fail 27/05/2022 is after CINclosureDate
            },
            {
                "LAchildID": "child1",
                "CINdetailsID": "CINID4",
                "CINclosureDate": "26/05/2022",
                "DateOfInitialCPC": "26/05/2022",  # pass
            },
        ]
    )

    df_ass = pd.DataFrame(
        [
            # multiple assessments in the same CIN. some pass, some fail.
            {
                "LAchildID": "child1",
                "CINdetailsID": "CINID1",
                "AssessmentActualStartDate": "27/05/2022",  # fail
                "AssessmentAuthorisationDate": "26/05/2022",  # pass
            },
            {
                "LAchildID": "child1",
                "CINdetailsID": "CINID1",
                "AssessmentActualStartDate": "26/05/2022",  # pass
                "AssessmentAuthorisationDate": "26/05/2022",  # pass
            },
            # assessments across multiple CIN modules. some pass, some fail.
            {
                "LAchildID": "child1",
                "CINdetailsID": "CINID2",
                "AssessmentActualStartDate": "27/05/2022",  # fail
                "AssessmentAuthorisationDate": "26/05/2022",  # pass
            },
            {
                "LAchildID": "child1",
                "CINdetailsID": "CINID3",
                "AssessmentActualStartDate": "26/05/2022",  # pass
                "AssessmentAuthorisationDate": "26/05/2022",  # pass
            },
            {
                "LAchildID": "child1",
                "CINdetailsID": "CINID4",
                "AssessmentActualStartDate": pd.NA,  # ignore. date is absent
                "AssessmentAuthorisationDate": pd.NA,  # ignore. date is absent
            },
        ]
    )

    df_47 = pd.DataFrame(
        [
            {
                "LAchildID": "child1",
                "CINdetailsID": "CINID1",
                "S47ActualStartDate": "27/05/2022",  # fail. after CINclosureDate.
                "DateOfInitialCPC": "26/05/2022",
            }
        ]
    )

    df_cpp = pd.DataFrame(
        [
            {
                "LAchildID": "child1",
                "CINdetailsID": "CINID1",
                "CPPendDate": "27/05/2022",  # fail
            }
        ]
    )

    df_plan = pd.DataFrame(
        [
            {
                "LAchildID": "child1",
                "CINdetailsID": "CINID1",
                "CINPlanStartDate": "27/05/2022",  # fail
                "CINPlanEndDate": "26/05/2022",  # pass
            }
        ]
    )

    df_plan["CINPlanStartDate"] = pd.to_datetime(
        df_plan["CINPlanStartDate"], format="%d/%m/%Y", errors="coerce"
    )
    df_plan["CINPlanEndDate"] = pd.to_datetime(
        df_plan["CINPlanEndDate"], format="%d/%m/%Y", errors="coerce"
    )

    df_cpp["CPPendDate"] = pd.to_datetime(
        df_cpp["CPPendDate"], format="%d/%m/%Y", errors="coerce"
    )

    df_47["S47ActualStartDate"] = pd.to_datetime(
        df_47["S47ActualStartDate"], format="%d/%m/%Y", errors="coerce"
    )
    df_47["DateOfInitialCPC"] = pd.to_datetime(
        df_47["DateOfInitialCPC"], format="%d/%m/%Y", errors="coerce"
    )

    df_ass["AssessmentActualStartDate"] = pd.to_datetime(
        df_ass["AssessmentActualStartDate"], format="%d/%m/%Y", errors="coerce"
    )
    df_ass["AssessmentAuthorisationDate"] = pd.to_datetime(
        df_ass["AssessmentAuthorisationDate"], format="%d/%m/%Y", errors="coerce"
    )

    df_cin["CINclosureDate"] = pd.to_datetime(
        df_cin["CINclosureDate"], format="%d/%m/%Y", errors="coerce"
    )
    df_cin["DateOfInitialCPC"] = pd.to_datetime(
        df_cin["DateOfInitialCPC"], format="%d/%m/%Y", errors="coerce"
    )

    result = run_rule(
        validate,
        {
            CINdetails: df_cin,
            Assessments: df_ass,
            Section47: df_47,
            ChildProtectionPlans: df_cpp,
            CINplanDates: df_plan,
        },
    )

    issues_list = result.type2_issues
    assert len(issues_list) == 5

    issues = issues_list[0]

    issue_table = issues.table
    assert issue_table == CINdetails

    issue_columns = issues.columns
    assert issue_columns == [
        CINclosureDate,
        DateOfInitialCPC,
    ]

    issue_rows = issues.row_df

    assert len(issue_rows) == 3
    assert isinstance(issue_rows, pd.DataFrame)
    assert issue_rows.columns.to_list() == ["ERROR_ID", "ROW_ID"]

    expected_df = pd.DataFrame(
        [
            {
                "ERROR_ID": (
                    "child1",
                    "CINID1",
                    pd.to_datetime("26/05/2022", format="%d/%m/%Y", errors="coerce"),
                ),
                "ROW_ID": [0],
            },
            {
                "ERROR_ID": (
                    "child1",
                    "CINID2",
                    pd.to_datetime("26/05/2022", format="%d/%m/%Y", errors="coerce"),
                ),
                "ROW_ID": [1],
            },
            {
                "ERROR_ID": (
                    "child1",
                    "CINID3",
                    pd.to_datetime("26/05/2022", format="%d/%m/%Y", errors="coerce"),
                ),
                "ROW_ID": [2],
            },
        ]
    )

    assert result.definition.code == "8565"
    assert result.definition.message == "Activity shown after a case has been closed"
