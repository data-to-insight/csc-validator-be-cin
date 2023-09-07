from typing import Mapping

import pandas as pd

from cin_validator.rule_engine import CINTable, RuleContext, rule_definition
from cin_validator.test_engine import run_rule
from cin_validator.utils import make_census_period

CINplanDates = CINTable.CINplanDates
LAchildID = CINplanDates.LAchildID
CINPlanStartDate = CINplanDates.CINPlanStartDate
CINPlanEndDate = CINplanDates.CINPlanEndDate

Header = CINTable.Header
ReferenceDate = Header.ReferenceDate


@rule_definition(
    code="4014",
    module=CINTable.CINplanDates,
    message="CIN Plan data contains overlapping dates",
    affected_fields=[
        CINPlanStartDate,
        CINPlanEndDate,
    ],
)
def validate(
    data_container: Mapping[CINTable, pd.DataFrame], rule_context: RuleContext
):
    df_cinp = data_container[CINplanDates].copy()
    df_cinp2 = data_container[CINplanDates].copy()

    df_cinp.index.name = "ROW_ID"
    df_cinp2.index.name = "ROW_ID"

    df_cinp.reset_index(inplace=True)
    df_cinp2.reset_index(inplace=True)

    df_ref = data_container[Header]
    ref_date_series = df_ref[ReferenceDate]

    collection_start, reference_date = make_census_period(ref_date_series)

    # Where more than one <CINplanDates> group is included,
    # the <CINPlanStartDate> (N00105) of each group cannot fall within either:
    #   a) <CINPlanStartDate> (N00105) to <CINPlanEndDate> (N00115), or
    #   b) <CINPlanStartDate> (N00105) and <ReferenceDate> if <CINPlanEndDate> (N00115) is not present
    # of any other group
    #
    # Issues dfs should return rows where CINPlanStartDate is between another CINPlanStartDate and CINPlanEndDate (or ReferenceDate)

    #  Create dataframes which only have rows with CIN plans, and which should have one plan per row.
    df_cinp = df_cinp[df_cinp[CINPlanStartDate].notna()]
    df_cinp2 = df_cinp2[df_cinp2[CINPlanStartDate].notna()]

    #  Merge tables to test for overlaps
    df_merged = df_cinp.merge(
        df_cinp2,
        on=["LAchildID"],
        how="left",
        suffixes=("_cinp", "_cinp2"),
    )

    # Use CINPlanStartDate to identify a CIN plan. Exclude rows where the ROW_ID is the same on both sides to prevent a plan from being compared with itself.
    df_merged = df_merged[df_merged["ROW_ID_cinp"] != df_merged["ROW_ID_cinp2"]]

    # Determine whether CINplanStart overlaps with another CINplan period of the same child.
    cinp_started_after_start = (
        df_merged["CINPlanStartDate_cinp"] >= df_merged["CINPlanStartDate_cinp2"]
    )
    cinp_started_before_end = (
        df_merged["CINPlanStartDate_cinp"] <= df_merged["CINPlanEndDate_cinp2"]
    ) & df_merged["CINPlanEndDate_cinp2"].notna()
    cinp_started_before_refdate = (
        df_merged["CINPlanStartDate_cinp"] <= reference_date
    ) & df_merged["CINPlanEndDate_cinp2"].isna()

    df_merged = df_merged[
        cinp_started_after_start
        & (cinp_started_before_end | cinp_started_before_refdate)
    ].reset_index()

    # create an identifier for each error instance.
    # In this case, the rule is checked for each CINPlanStartDate, in each CPplanDates group (differentiated by CP dates), in each child (differentiated by LAchildID)
    df_merged["ERROR_ID"] = tuple(
        zip(
            df_merged[LAchildID],
            df_merged["CINPlanStartDate_cinp"],
            df_merged["CINPlanStartDate_cinp2"],
        )
    )

    # The merges were done on copies of cinp_df so that the column names in dataframes themselves aren't affected by the suffixes.
    # we can now map the suffixes columns to their corresponding source tables such that the failing ROW_IDs and ERROR_IDs exist per table.
    df_cinp_issues = (
        df_cinp.merge(df_merged, left_on="ROW_ID", right_on="ROW_ID_cinp")
        .groupby("ERROR_ID", group_keys=False)["ROW_ID"]
        .apply(list)
        .reset_index()
    )

    df_cinp2_issues = (
        df_cinp2.merge(df_merged, left_on="ROW_ID", right_on="ROW_ID_cinp2")
        .groupby("ERROR_ID", group_keys=False)["ROW_ID"]
        .apply(list)
        .reset_index()
    )

    # Ensure that you maintain the ROW_ID, and ERROR_ID column names which are shown above. They are keywords in this project.
    rule_context.push_type_3(
        table=CINplanDates, columns=[CINPlanStartDate], row_df=df_cinp_issues
    )
    rule_context.push_type_3(
        table=CINplanDates,
        columns=[CINPlanStartDate, CINPlanEndDate],
        row_df=df_cinp2_issues,
    )


def test_validate():
    # Create some sample data such that some values pass the validation and some fail.
    sample_header = pd.DataFrame(
        [{ReferenceDate: "31/03/2001"}]  # the census start date here will be 01/04/2000
    )

    sample_cinp = pd.DataFrame(
        [
            {
                "LAchildID": "child1",  # 0 Pass
                "CINPlanStartDate": "26/05/2000",
                "CINPlanEndDate": "26/10/2000",
            },
            {
                "LAchildID": "child1",  # 1 Fail because of 0
                "CINPlanStartDate": "26/08/2000",
                "CINPlanEndDate": "26/12/2000",
            },
            {
                "LAchildID": "child2",  # 2 Pass
                "CINPlanStartDate": "26/05/2000",
                "CINPlanEndDate": "25/10/2000",
            },
            {
                "LAchildID": "child2",  # 3 Pass
                "CINPlanStartDate": "26/10/2000",
                "CINPlanEndDate": "26/12/2000",
            },
            {
                "LAchildID": "child3",  # 4 Pass
                "CINPlanStartDate": "26/05/2000",
                "CINPlanEndDate": pd.NA,
            },
            {
                "LAchildID": "child3",  # 5 Fail
                "CINPlanStartDate": "26/08/2000",
                "CINPlanEndDate": "26/10/2000",
            },
            {
                "LAchildID": "child4",  # 6 Pass
                "CINPlanStartDate": "26/10/2000",
                "CINPlanEndDate": "31/03/2001",
            },
            {
                "LAchildID": "child4",  # 7 Fail
                "CINPlanStartDate": "31/03/2001",
                "CINPlanEndDate": pd.NA,
            },
            {
                "LAchildID": "child5",  # 8 Fail
                "CINPlanStartDate": "31/03/2001",
                "CINPlanEndDate": "31/04/2001",
            },
            {
                "LAchildID": "child5",  # 9 Fail
                "CINPlanStartDate": "31/03/2001",
                "CINPlanEndDate": "31/04/2001",
            },
        ]
    )

    # If rule requires columns containing date values, convert those columns to datetime objects first. Do it here in the test_validate function, not above.
    sample_cinp[CINPlanStartDate] = pd.to_datetime(
        sample_cinp[CINPlanStartDate], format="%d/%m/%Y", errors="coerce"
    )
    sample_cinp["CINPlanEndDate"] = pd.to_datetime(
        sample_cinp["CINPlanEndDate"], format="%d/%m/%Y", errors="coerce"
    )
    sample_header[ReferenceDate] = pd.to_datetime(
        sample_header[ReferenceDate], format="%d/%m/%Y", errors="coerce"
    )

    result = run_rule(
        validate,
        {
            CINplanDates: sample_cinp,
            Header: sample_header,
        },
    )

    issues_list = result.type3_issues
    assert len(issues_list) == 2

    issues = issues_list[0]

    issue_table = issues.table
    assert issue_table == CINplanDates

    issue_columns = issues.columns
    assert issue_columns == [CINPlanStartDate]

    issue_rows = issues.row_df
    assert len(issue_rows) == 4

    assert isinstance(issue_rows, pd.DataFrame)
    assert issue_rows.columns.to_list() == ["ERROR_ID", "ROW_ID"]

    expected_df = pd.DataFrame(
        [
            {
                "ERROR_ID": (
                    "child1",
                    pd.to_datetime("26/08/2000", format="%d/%m/%Y", errors="coerce"),
                    pd.to_datetime("26/05/2000", format="%d/%m/%Y", errors="coerce"),
                ),
                "ROW_ID": [1],
            },
            {
                "ERROR_ID": (
                    "child3",
                    pd.to_datetime("26/08/2000", format="%d/%m/%Y", errors="coerce"),
                    pd.to_datetime("26/05/2000", format="%d/%m/%Y", errors="coerce"),
                ),
                "ROW_ID": [5],
            },
            {
                "ERROR_ID": (
                    "child4",
                    pd.to_datetime("31/03/2001", format="%d/%m/%Y", errors="coerce"),
                    pd.to_datetime("26/10/2000", format="%d/%m/%Y", errors="coerce"),
                ),
                "ROW_ID": [7],
            },
            {
                "ERROR_ID": (
                    "child5",
                    pd.to_datetime("31/03/2001", format="%d/%m/%Y", errors="coerce"),
                    pd.to_datetime("31/03/2001", format="%d/%m/%Y", errors="coerce"),
                ),
                "ROW_ID": [8, 9],
            },
        ]
    )
    assert issue_rows.equals(expected_df)

    assert result.definition.code == "4014"
    assert result.definition.message == "CIN Plan data contains overlapping dates"
