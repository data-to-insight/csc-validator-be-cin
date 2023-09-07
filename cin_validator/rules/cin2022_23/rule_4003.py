from typing import Mapping

import pandas as pd

from cin_validator.rule_engine import CINTable, RuleContext, rule_definition
from cin_validator.test_engine import run_rule

CINplanDates = CINTable.CINplanDates
Reviews = CINTable.Reviews
ChildProtectionPlans = CINTable.ChildProtectionPlans
CPPendDate = ChildProtectionPlans.CPPendDate
LAchildID = CINplanDates.LAchildID
CPPreviewDate = Reviews.CPPreviewDate
CPPID = Reviews.CPPID
CINPlanStartDate = CINplanDates.CINPlanStartDate
CINPlanEndDate = CINplanDates.CINPlanEndDate
CPPendDate = ChildProtectionPlans.CPPendDate


@rule_definition(
    code="4003",
    module=CINTable.Reviews,
    message="A CPP review date is shown as being held at the same time as an open CIN Plan.",
    affected_fields=[
        CINPlanStartDate,
        CINPlanEndDate,
        CPPreviewDate,
    ],
)
def validate(
    data_container: Mapping[CINTable, pd.DataFrame], rule_context: RuleContext
):
    df_cpp = data_container[ChildProtectionPlans].copy()
    df_cin = data_container[CINplanDates].copy()
    df_reviews = data_container[Reviews].copy()

    df_reviews.index.name = "ROW_ID"
    df_cpp.index.name = "ROW_ID"
    df_cin.index.name = "ROW_ID"

    df_reviews.reset_index(inplace=True)
    df_cpp.reset_index(inplace=True)
    df_cin.reset_index(inplace=True)

    # Within a <CINDetails> module, no <CPPReviewDate> (N00116) can fall between any
    # <CINPlanStartdate> (N00689) or <CINPlanEndDate> (N00690) unless <CPPReviewDate> is equal to <CPPendDate> (N00115)
    df_cpp = df_cpp.merge(
        df_reviews, on=["LAchildID", "CPPID"], how="left", suffixes=("", "_reviews")
    )

    df_merged = df_cin.merge(
        df_cpp,
        on=["LAchildID"],
        how="left",
        suffixes=("_cin", "_cpp"),
    )

    cin_start_after_cin_start = df_merged[CPPreviewDate] >= df_merged[CINPlanStartDate]
    cin_start_before_cin_end = (
        df_merged[CPPreviewDate] < df_merged[CINPlanEndDate]
    ) & df_merged[CPPendDate].notna()
    cp_review_is_end = (df_merged[CPPreviewDate] == df_merged[CPPendDate]) & df_merged[
        CPPendDate
    ].notna()

    df_merged = df_merged[
        (cin_start_after_cin_start & cin_start_before_cin_end) & (~cp_review_is_end)
    ].reset_index()

    df_merged["ERROR_ID"] = tuple(zip(df_merged[LAchildID], df_merged[CPPreviewDate]))
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
    df_reviews_issues = (
        df_reviews.merge(df_merged, left_on="ROW_ID", right_on="ROW_ID_reviews")
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
        table=CINplanDates, columns=[CINPlanStartDate], row_df=df_cin_issues
    )
    rule_context.push_type_2(
        table=Reviews, columns=[CPPreviewDate], row_df=df_reviews_issues
    )


def test_validate():
    sample_cpp = pd.DataFrame(
        [
            {
                "LAchildID": "child1",
                "CPPendDate": "30/05/2001",  # Fail
                "CPPID": "cinID1",
            },
            {
                "LAchildID": "child2",
                "CPPendDate": pd.NA,
                "CPPID": "cinID1",
            },
            {
                "LAchildID": "child2",
                "CPPendDate": "29/05/2001",  # ignore: CPPReviewDate == CPPendDate
                "CPPID": "cinID2",
            },
            {
                "LAchildID": "child2",
                "CPPendDate": pd.NA,
                "CPPID": "cinID4",
            },
            {
                "LAchildID": "child3",
                "CPPendDate": "30/10/2001",
                "CPPID": "cinID5",
            },
            {
                "LAchildID": "child5",
                "CPPendDate": pd.NA,
                "CPPID": "cinID6",
            },
        ]
    )
    sample_cin = pd.DataFrame(
        [
            {
                "LAchildID": "child1",
                "CINPlanStartDate": "04/04/2000",  # fail: 29/05/2001
                "CINPlanEndDate": "01/06/2002",
            },
            {
                "LAchildID": "child2",
                "CINPlanStartDate": "28/05/2000",  # ignore: CPPReviewDate == CPPendDate 29/05/2001
                "CINPlanEndDate": "01/06/2002",
            },
            {
                "LAchildID": "child3",
                "CINPlanStartDate": "30/05/2000",  # pass: "29/05/2004"
                "CINPlanEndDate": "01/06/2002",
            },
            {
                "LAchildID": "child4",
                "CINPlanStartDate": "04/06/2000",  # pass: "29/05/2004"
                "CINPlanEndDate": "01/06/2002",
            },
            {
                "LAchildID": "child5",
                "CINPlanStartDate": "30/06/2000",
                "CINPlanEndDate": "01/06/2002",
            },
            {
                "LAchildID": "child2",
                "CINPlanStartDate": "26/10/2000",
                "CINPlanEndDate": "01/06/2002",
            },
            {
                "LAchildID": "child2",
                "CINPlanStartDate": "26/02/2001",
                "CINPlanEndDate": "01/06/2002",
            },
            {
                "LAchildID": "child2",
                "CINPlanStartDate": "26/03/2001",
                "CINPlanEndDate": "01/06/2002",
            },
            {
                "LAchildID": "child3",
                "CINPlanStartDate": "30/10/2001",
                "CINPlanEndDate": "01/06/2002",
            },
            {
                "LAchildID": "child4",
                "CINPlanStartDate": "04/06/2000",
                "CINPlanEndDate": "01/06/2002",
            },
            {
                "LAchildID": "child5",
                "CINPlanStartDate": "31/03/2001",
                "CINPlanEndDate": "01/06/2002",
            },
        ]
    )
    sample_reviews = pd.DataFrame(
        [
            {
                "LAchildID": "child1",
                "CPPID": "cinID1",
                "CPPreviewDate": "29/05/2001",
            },  # fail
            {
                "LAchildID": "child2",
                "CPPID": "cinID2",
                "CPPreviewDate": "29/05/2001",
            },  # ignore
            {
                "LAchildID": "child3",
                "CPPID": "cinID3",
                "CPPreviewDate": "29/05/2004",
            },  # pass
            {
                "LAchildID": "child4",
                "CPPID": "cinID4",
                "CPPreviewDate": "29/05/2004",
            },  # pass
            {
                "LAchildID": "child5",
                "CPPID": "cinID5",
                "CPPreviewDate": "29/05/2004",
            },  # pass
            {
                "LAchildID": "child6",
                "CPPID": "cinID6",
                "CPPreviewDate": "29/05/2004",
            },  # ignore: not present in cin table
        ]
    )

    sample_reviews[CPPreviewDate] = pd.to_datetime(
        sample_reviews[CPPreviewDate], format="%d/%m/%Y", errors="coerce"
    )
    sample_cpp[CPPendDate] = pd.to_datetime(
        sample_cpp[CPPendDate], format="%d/%m/%Y", errors="coerce"
    )
    sample_cin[CINPlanStartDate] = pd.to_datetime(
        sample_cin[CINPlanStartDate], format="%d/%m/%Y", errors="coerce"
    )
    sample_cin[CINPlanEndDate] = pd.to_datetime(
        sample_cin[CINPlanEndDate], format="%d/%m/%Y", errors="coerce"
    )

    result = run_rule(
        validate,
        {
            ChildProtectionPlans: sample_cpp,
            CINplanDates: sample_cin,
            Reviews: sample_reviews,
        },
    )

    issues_list = result.type2_issues
    assert len(issues_list) == 3
    issues = issues_list[2]

    issue_table = issues.table
    assert issue_table == Reviews

    issue_columns = issues.columns
    assert issue_columns == [CPPreviewDate]

    issue_rows = issues.row_df
    assert len(issue_rows) == 1
    assert isinstance(issue_rows, pd.DataFrame)
    assert issue_rows.columns.to_list() == ["ERROR_ID", "ROW_ID"]

    expected_df = pd.DataFrame(
        [
            {
                "ERROR_ID": (
                    "child1",
                    pd.to_datetime("29/05/2001", format="%d/%m/%Y", errors="coerce"),
                ),
                "ROW_ID": [0],
            }
        ]
    )

    assert issue_rows.equals(expected_df)
    assert result.definition.code == "4003"
    assert (
        result.definition.message
        == "A CPP review date is shown as being held at the same time as an open CIN Plan."
    )
