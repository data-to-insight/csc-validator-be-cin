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

Header = CINTable.Header
ReferenceDate = Header.ReferenceDate


@rule_definition(
    code="4016",
    module=CINTable.CINplanDates,
    message="A CIN Plan has been reported as open at the same time as a Child Protection Plan.",
    affected_fields=[
        CINPlanStartDate,
        CPPstartDate,
        CPPendDate,
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

    # The <CINPlanStartDate> (N00689) for any CIN Plan group cannot fall within either:
    # <CPPstartDate> (N00105) or <CPPendDate> (N00115);
    # or <CPPstartDate> and <ReferenceDate> (N00603) if <CPPendDate> is not present - for any CPP group;
    # unless <CINPlanStartDate> is equal to <CPPendDate> for this group

    #  Merge tables
    df_merged = df_cin.merge(
        df_cpp,
        on=["LAchildID"],
        how="left",
        suffixes=("_cin", "_cpp"),
    )

    cin_start_after_cpp_start = df_merged[CINPlanStartDate] >= df_merged[CPPstartDate]
    cin_start_before_cpp_end = (
        df_merged[CINPlanStartDate] < df_merged[CPPendDate]
    ) & df_merged[CPPendDate].notna()
    cin_start_before_reference_date = (
        df_merged[CINPlanStartDate] <= reference_date
    ) & df_merged[CPPendDate].isna()

    df_merged = df_merged[
        cin_start_after_cpp_start
        & (cin_start_before_cpp_end | cin_start_before_reference_date)
    ].reset_index()

    df_merged["ERROR_ID"] = tuple(
        zip(df_merged[LAchildID], df_merged[CINPlanStartDate])
    )

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
        columns=[CPPstartDate, CPPendDate],
        row_df=df_cpp_issues,
    )
    rule_context.push_type_2(
        table=CINplanDates, columns=[CINPlanStartDate], row_df=df_cin_issues
    )


def test_validate():
    sample_header = pd.DataFrame(
        [{ReferenceDate: "31/03/2001"}]  # the census start date here will be 01/04/2000
    )

    sample_cpp = pd.DataFrame(
        [
            {
                "LAchildID": "child1",
                "CPPstartDate": "26/05/2000",
                "CPPendDate": "30/05/2000",
                "CPPID": "cinID1",
            },
            {
                "LAchildID": "child1",
                "CPPstartDate": "26/06/2000",
                "CPPendDate": pd.NA,
                "CPPID": "cinID1",
            },
            {
                "LAchildID": "child2",
                "CPPstartDate": "26/10/2000",
                "CPPendDate": "10/12/2000",
                "CPPID": "cinID1",
            },
            {
                "LAchildID": "child2",
                "CPPstartDate": "26/02/2001",
                "CPPendDate": pd.NA,
                "CPPID": "cinID1",
            },
            {
                "LAchildID": "child3",
                "CPPstartDate": "26/05/2000",
                "CPPendDate": "30/10/2001",
                "CPPID": "cinID1",
            },
            {
                "LAchildID": "child5",
                "CPPstartDate": "26/05/2000",
                "CPPendDate": pd.NA,
                "CPPID": "cinID1",
            },
        ]
    )
    sample_cin = pd.DataFrame(
        [
            {
                "LAchildID": "child1",  # 0 Pass - Before CPP
                "CINPlanStartDate": "04/04/2000",
            },
            {
                "LAchildID": "child1",  # 1 Fail - During CPP
                "CINPlanStartDate": "28/05/2000",
            },
            {
                "LAchildID": "child1",  # 2 Pass - Same as CPP End
                "CINPlanStartDate": "30/05/2000",
            },
            {
                "LAchildID": "child1",  # 3 Pass - Between CPPs
                "CINPlanStartDate": "04/06/2000",
            },
            {
                "LAchildID": "child1",  # 4 Fail - During CPP (via reference_date)
                "CINPlanStartDate": "30/06/2000",
            },
            {
                "LAchildID": "child2",  # 5 Fail - Same as CPP Start
                "CINPlanStartDate": "26/10/2000",
            },
            {
                "LAchildID": "child2",  # 6 Fail - Same as CPP Start
                "CINPlanStartDate": "26/02/2001",
            },
            {
                "LAchildID": "child2",  # 7 Fail - During CPP (via reference_date)
                "CINPlanStartDate": "26/03/2001",
            },
            {
                "LAchildID": "child3",  # 8 Pass - Same as CPP End (future return year handled by different rule!)
                "CINPlanStartDate": "30/10/2001",
            },
            {
                "LAchildID": "child4",  # 9 Pass - No CPP
                "CINPlanStartDate": "04/06/2000",
            },
            {
                "LAchildID": "child5",  # 10 Fail - Start on ReferenceDate
                "CINPlanStartDate": "31/03/2001",
            },
        ]
    )

    sample_cpp[CPPstartDate] = pd.to_datetime(
        sample_cpp[CPPstartDate], format="%d/%m/%Y", errors="coerce"
    )
    sample_cpp[CPPendDate] = pd.to_datetime(
        sample_cpp[CPPendDate], format="%d/%m/%Y", errors="coerce"
    )
    sample_cin[CINPlanStartDate] = pd.to_datetime(
        sample_cin[CINPlanStartDate], format="%d/%m/%Y", errors="coerce"
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
    issues = issues_list[1]

    issue_table = issues.table
    assert issue_table == CINplanDates

    issue_columns = issues.columns
    assert issue_columns == [CINPlanStartDate]

    issue_rows = issues.row_df
    assert len(issue_rows) == 6
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
        ]
    )
    assert issue_rows.equals(expected_df)

    assert result.definition.code == "4016"
    assert (
        result.definition.message
        == "A CIN Plan has been reported as open at the same time as a Child Protection Plan."
    )
