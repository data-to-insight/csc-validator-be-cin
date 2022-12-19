from typing import Mapping

import pandas as pd

from cin_validator.rule_engine import CINTable, RuleContext, rule_definition
from cin_validator.test_engine import run_rule
from cin_validator.utils import make_census_period

# Get tables and columns of interest from the CINTable object defined in rule_engine/__api.py

CINplanDates = CINTable.CINplanDates
LAchildID = CINplanDates.LAchildID
CINPlanStartDate = CINplanDates.CINPlanStartDate
CINPlanEndDate = CINplanDates.CINPlanEndDate

Header = CINTable.Header
ReferenceDate = Header.ReferenceDate

# define characteristics of rule
@rule_definition(
    # write the rule code here
    code=4014,
    # replace CINplanDates with the value in the module column of the excel sheet corresponding to this rule .
    # Note that even if multiple tables are involved, one table will be named in the module column.
    module=CINTable.CINplanDates,
    # replace the message with the corresponding value for this rule, gotten from the excel sheet.
    message="CIN Plan data contains overlapping dates",
    # The column names tend to be the words within the < > signs in the github issue description.
    affected_fields=[
        CINPlanStartDate,
        CINPlanEndDate,
    ],
)
def validate(
    data_container: Mapping[CINTable, pd.DataFrame], rule_context: RuleContext
):
    # PREPARING DATA

    # Replace CINplanDates with the name of the table you need.
    df_cinp = data_container[CINplanDates].copy()
    df_cinp2 = data_container[CINplanDates].copy()

    # Before you begin, rename the index so that the initial row positions can be kept intact.
    df_cinp.index.name = "ROW_ID"
    df_cinp2.index.name = "ROW_ID"

    # Resetting the index causes the ROW_IDs to become columns of their respective DataFrames
    # so that they can come along when the merge is done.
    df_cinp.reset_index(inplace=True)
    df_cinp2.reset_index(inplace=True)

    # ReferenceDate exists in the header table so we get header table too.
    df_ref = data_container[Header]
    ref_date_series = df_ref[ReferenceDate]

    # the make_census_period function generates the start and end date so that you don't have to do it each time.
    collection_start, reference_date = make_census_period(ref_date_series)

    # lOGIC
    # Implement rule logic as described by the Github issue.
    # Put the description as a comment above the implementation as shown.

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

    # Use CINPlanStartDate to identify a CIN plan. Exclude rows where the CINPlanStartDate is the same on both sides to prevent a plan from being compared with itself.
    df_merged = df_merged[
        df_merged["CINPlanStartDate_cinp"] != df_merged["CINPlanStartDate_cinp2"]
    ]

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

    # Run the rule function, passing in our sample data.
    result = run_rule(
        validate,
        {
            CINplanDates: sample_cinp,
            Header: sample_header,
        },
    )

    # Use .type2_issues to check for the result of .push_type2_issues() which you used above.
    issues_list = result.type3_issues
    assert len(issues_list) == 2
    # the function returns a list on NamedTuples where each NamedTuple contains (table, column_list, df_issues)
    # pick any table and check it's values. the tuple in location 1 will contain the Reviews columns because that's the second thing pushed above.
    issues = issues_list[0]

    # get table name and check it. Replace Reviews with the name of your table.
    issue_table = issues.table
    assert issue_table == CINplanDates

    # check that the right columns were returned. Replace CINPlanStartDate  with a list of your columns.
    issue_columns = issues.columns
    assert issue_columns == [CINPlanStartDate]

    # check that the location linking dataframe was formed properly.
    issue_rows = issues.row_df
    # replace 3 with the number of failing points you expect from the sample data.
    assert len(issue_rows) == 3

    # check that the failing locations are contained in a DataFrame having the appropriate columns. These lines do not change.
    assert isinstance(issue_rows, pd.DataFrame)
    assert issue_rows.columns.to_list() == ["ERROR_ID", "ROW_ID"]

    # Create the dataframe which you expect, based on the fake data you created. It should have two columns.
    # - The first column is ERROR_ID which contains the unique combination that identifies each error instance, which you decided on, in your zip, earlier.
    # - The second column in ROW_ID which contains a list of index positions that belong to each error instance.

    # The ROW ID values represent the index positions where you expect the sample data to fail the validation check.
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
        ]
    )
    assert issue_rows.equals(expected_df)

    # Check that the rule definition is what you wrote in the context above.

    # replace 2885 with the rule code and put the appropriate message in its place too.
    assert result.definition.code == 4014
    assert result.definition.message == "CIN Plan data contains overlapping dates"
