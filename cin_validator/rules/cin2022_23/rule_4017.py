from typing import Mapping

import pandas as pd

from cin_validator.rule_engine import CINTable, RuleContext, rule_definition
from cin_validator.test_engine import run_rule
from cin_validator.utils import make_census_period

ChildProtectionPlans = CINTable.ChildProtectionPlans
LAchildID = ChildProtectionPlans.LAchildID
CPPID = ChildProtectionPlans.CPPID
CPPstartDate = ChildProtectionPlans.CPPstartDate
CPPendDate = ChildProtectionPlans.CPPendDate

CINplanDates = CINTable.CINplanDates
CINPlanStartDate = CINplanDates.CINPlanStartDate
CINPlanEndDate = CINplanDates.CINPlanEndDate

Header = CINTable.Header
ReferenceDate = Header.ReferenceDate


@rule_definition(
    code="4017",
    module=CINTable.CINplanDates,
    message="A CIN Plan has been reported as open at the same time as a Child Protection Plan.",
    affected_fields=[
        CINPlanStartDate,
        CINPlanEndDate,
        CPPstartDate,
    ],
)
def validate(
    data_container: Mapping[CINTable, pd.DataFrame], rule_context: RuleContext
):
    df_cpp = data_container[ChildProtectionPlans].copy()
    df_cin = data_container[CINplanDates].copy()

    df_cpp.index.name = "ROW_ID"
    df_cin.index.name = "ROW_ID"

    df_cpp.reset_index(inplace=True)
    df_cin.reset_index(inplace=True)

    df_ref = data_container[Header]
    ref_date_series = df_ref[ReferenceDate]

    collection_start, reference_date = make_census_period(ref_date_series)

    # The <CPPstartDate> (N00105) for any CPP group cannot fall within either:
    # <CINPlanStartDate> (N00689) or <CINPlanEndDate> (N00690);
    # or <CINPlanStartDate> and <ReferenceDate> (N00603) if <CINPlanEndDate> is not present - for any CIN Plan Group;
    # unless <CCPstartDate> is equal to <CINPlanEndDate> for this group.

    #  Merge tables
    df_merged = df_cpp.merge(
        df_cin,
        on=["LAchildID"],
        how="left",
        suffixes=("_cpp", "_cin"),
    )

    # Get rows where CPPstartDate is after CINPlanStartDate
    # and CPPstartDate before CINPlanEndDate (or if null, before/on ReferenceDate)
    cpp_start_after_cin_start = df_merged[CPPstartDate] >= df_merged[CINPlanStartDate]
    cpp_start_before_cin_end = (
        df_merged[CPPstartDate] < df_merged[CINPlanEndDate]
    ) & df_merged[CINPlanEndDate].notna()
    cpp_start_before_reference_date = (
        df_merged[CPPstartDate] <= reference_date
    ) & df_merged[CINPlanEndDate].isna()

    df_merged = df_merged[
        cpp_start_after_cin_start
        & (cpp_start_before_cin_end | cpp_start_before_reference_date)
    ].reset_index()

    df_merged["ERROR_ID"] = tuple(zip(df_merged[LAchildID], df_merged[CPPstartDate]))

    df_cpp_issues = (
        df_cpp.merge(df_merged, left_on="ROW_ID", right_on="ROW_ID_cpp")
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
        table=ChildProtectionPlans,
        columns=[CPPstartDate],
        row_df=df_cpp_issues,
    )
    rule_context.push_type_2(
        table=CINplanDates,
        columns=[CINPlanStartDate, CINPlanEndDate],
        row_df=df_cin_issues,
    )


def test_validate():
    sample_header = pd.DataFrame(
        [{ReferenceDate: "31/03/2001"}]  # the census start date here will be 01/04/2000
    )

    sample_cin = pd.DataFrame(
        [
            {
                "LAchildID": "child1",
                "CINPlanStartDate": "26/05/2000",
                "CINPlanEndDate": "30/05/2000",
            },
            {
                "LAchildID": "child1",
                "CINPlanStartDate": "26/06/2000",
                "CINPlanEndDate": pd.NA,
            },
            {
                "LAchildID": "child2",
                "CINPlanStartDate": "26/10/2000",
                "CINPlanEndDate": "10/12/2000",
            },
            {
                "LAchildID": "child2",
                "CINPlanStartDate": "26/02/2001",
                "CINPlanEndDate": pd.NA,
            },
            {
                "LAchildID": "child3",
                "CINPlanStartDate": "26/05/2000",
                "CINPlanEndDate": "30/10/2001",
            },
            {
                "LAchildID": "child5",
                "CINPlanStartDate": "26/05/2000",
                "CINPlanEndDate": pd.NA,
            },
            {
                "LAchildID": "child8",
                "CINPlanStartDate": "15/09/2000",
                "CINPlanEndDate": pd.NA,
            },
            {
                "LAchildID": "child8",
                "CINPlanStartDate": "09/06/2000",
                "CINPlanEndDate": "20/07/2000",
            },
        ]
    )
    sample_cpp = pd.DataFrame(
        [
            {
                "LAchildID": "child1",  # 0 Pass - Before CIN
                "CPPstartDate": "04/04/2000",
            },
            {
                "LAchildID": "child1",  # 1 Fail - During CIN
                "CPPstartDate": "28/05/2000",
            },
            {
                "LAchildID": "child1",  # 2 Pass - Same as CIN End
                "CPPstartDate": "30/05/2000",
            },
            {
                "LAchildID": "child1",  # 3 Pass - Between CIN
                "CPPstartDate": "04/06/2000",
            },
            {
                "LAchildID": "child1",  # 4 Fail - During CIN (via reference_date)
                "CPPstartDate": "30/06/2000",
            },
            {
                "LAchildID": "child2",  # 5 Fail - Same as CIN Start
                "CPPstartDate": "26/10/2000",
            },
            {
                "LAchildID": "child2",  # 6 Fail - Same as CIN Start
                "CPPstartDate": "26/02/2001",
            },
            {
                "LAchildID": "child2",  # 7 Fail - During CIN (via reference_date)
                "CPPstartDate": "26/03/2001",
            },
            {
                "LAchildID": "child3",  # 8 Pass - Same as CIN End (future return year handled by different rule!)
                "CPPstartDate": "30/10/2001",
            },
            {
                "LAchildID": "child4",  # 9 Pass - No CIN
                "CPPstartDate": "04/06/2000",
            },
            {
                "LAchildID": "child5",  # 10 Fail - Start on ReferenceDate
                "CPPstartDate": "31/03/2001",
            },
            {
                "LAchildID": "child8",  # 11 Fail - between CinStartDate and CinEndDate
                "CPPstartDate": "22/06/2000",
            },
        ]
    )

    sample_cpp[CPPstartDate] = pd.to_datetime(
        sample_cpp[CPPstartDate], format="%d/%m/%Y", errors="coerce"
    )
    sample_cin[CINPlanStartDate] = pd.to_datetime(
        sample_cin[CINPlanStartDate], format="%d/%m/%Y", errors="coerce"
    )
    sample_cin[CINPlanEndDate] = pd.to_datetime(
        sample_cin[CINPlanEndDate], format="%d/%m/%Y", errors="coerce"
    )
    sample_header[ReferenceDate] = pd.to_datetime(
        sample_header[ReferenceDate], format="%d/%m/%Y", errors="coerce"
    )

    result = run_rule(
        validate,
        {
            ChildProtectionPlans: sample_cpp,
            CINplanDates: sample_cin,
            Header: sample_header,
        },
    )

    issues_list = result.type2_issues
    assert len(issues_list) == 2
    issues = issues_list[0]

    issue_table = issues.table
    assert issue_table == ChildProtectionPlans

    issue_columns = issues.columns
    assert issue_columns == [CPPstartDate]

    issue_rows = issues.row_df
    assert len(issue_rows) == 7
    assert isinstance(issue_rows, pd.DataFrame)
    assert issue_rows.columns.to_list() == ["ERROR_ID", "ROW_ID"]

    expected_df = pd.DataFrame(
        [
            {
                "ERROR_ID": (
                    "child1",
                    pd.to_datetime("28/05/2000", format="%d/%m/%Y", errors="coerce"),
                ),
                "ROW_ID": [1],
            },
            {
                "ERROR_ID": (
                    "child1",
                    pd.to_datetime("30/06/2000", format="%d/%m/%Y", errors="coerce"),
                ),
                "ROW_ID": [4],
            },
            {
                "ERROR_ID": (
                    "child2",
                    pd.to_datetime("26/10/2000", format="%d/%m/%Y", errors="coerce"),
                ),
                "ROW_ID": [5],
            },
            {
                "ERROR_ID": (
                    "child2",
                    pd.to_datetime("26/02/2001", format="%d/%m/%Y", errors="coerce"),
                ),
                "ROW_ID": [6],
            },
            {
                "ERROR_ID": (
                    "child2",
                    pd.to_datetime("26/03/2001", format="%d/%m/%Y", errors="coerce"),
                ),
                "ROW_ID": [7],
            },
            {
                "ERROR_ID": (
                    "child5",
                    pd.to_datetime("31/03/2001", format="%d/%m/%Y", errors="coerce"),
                ),
                "ROW_ID": [10],
            },
            {
                "ERROR_ID": (
                    "child8",
                    pd.to_datetime("22/06/2000", format="%d/%m/%Y", errors="coerce"),
                ),
                "ROW_ID": [11],
            },
        ]
    )
    assert issue_rows.equals(expected_df)

    assert result.definition.code == "4017"
    assert (
        result.definition.message
        == "A CIN Plan has been reported as open at the same time as a Child Protection Plan."
    )
