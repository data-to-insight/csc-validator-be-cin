from typing import Mapping

import pandas as pd

from cin_validator.rule_engine import CINTable, RuleContext, rule_definition
from cin_validator.test_engine import run_rule
from cin_validator.utils import make_census_period

# Get tables and columns of interest from the CINTable object defined in rule_engine/__api.py

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


# define characteristics of rule
@rule_definition(
    # write the rule code here
    code=4003,
    # replace ChildProtectionPlans with the value in the module column of the excel sheet corresponding to this rule .
    # Note that even if multiple tables are involved, one table will be named in the module column.
    module=CINTable.Reviews,
    # replace the message with the corresponding value for this rule, gotten from the excel sheet.
    message="A CPP review date is shown as being held at the same time as an open CIN Plan.",
    # The column names tend to be the words within the < > signs in the github issue description.
    affected_fields=[
        CINPlanStartDate,
        CINPlanEndDate,
        CPPreviewDate,
    ],
)
def validate(
    data_container: Mapping[CINTable, pd.DataFrame], rule_context: RuleContext
):
    # PREPARING DATA

    # Replace ChildProtectionPlans with the name of the table you need.
    df_cpp = data_container[ChildProtectionPlans].copy()
    df_cin = data_container[CINplanDates].copy()
    df_reviews = data_container[Reviews].copy()

    # Before you begin, rename the index so that the initial row positions can be kept intact.
    df_reviews.index.name = "ROW_ID"
    df_cpp.index.name = "ROW_ID"
    df_cin.index.name = "ROW_ID"

    # Resetting the index causes the ROW_IDs to become columns of their respective DataFrames
    # so that they can come along when the merge is done.
    df_reviews.reset_index(inplace=True)
    df_cpp.reset_index(inplace=True)
    df_cin.reset_index(inplace=True)

    # lOGIC
    # Within a <CINDetails> module, no <CPPReviewDate> (N00116) can fall between any
    # <CINPlanStartdate> (N00689) or <CINPlanEndDate> (N00690) unless <CPPReviewDate> is equal to <CPPendDate> (N00115)

    df_cpp = df_cpp.merge(
        df_reviews, on=["LAchildID", "CPPID"], how="left", suffixes=("", "_reviews")
    )

    #  Merge tables
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

    # create an identifier for each error instance.
    df_merged["ERROR_ID"] = tuple(zip(df_merged[LAchildID], df_merged[CPPreviewDate]))
    # The merges were done on copies of df_cpp and df_cin so that the column names in dataframes themselves aren't affected by the suffixes.
    # we can now map the suffixes columns to their corresponding source tables such that the failing ROW_IDs and ERROR_IDs exist per table.
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
    print(df_reviews_issues)
    # Ensure that you maintain the ROW_ID, and ERROR_ID column names which are shown above. They are keywords in this project.
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
    # Create some sample data such that some values pass the validation and some fail.

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
                "CPPID": "cinID2",
            },
            {
                "LAchildID": "child2",
                "CPPendDate": "29/05/2001",
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
                "CINPlanStartDate": "04/04/2000",
                "CINPlanEndDate": "01/06/2002",
            },
            {
                "LAchildID": "child2",
                "CINPlanStartDate": "28/05/2000",
                "CINPlanEndDate": "01/06/2002",
            },
            {
                "LAchildID": "child3",
                "CINPlanStartDate": "30/05/2000",
                "CINPlanEndDate": "01/06/2002",
            },
            {
                "LAchildID": "child4",
                "CINPlanStartDate": "04/06/2000",
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
            {"LAchildID": "child1", "CPPID": "cinID1", "CPPreviewDate": "29/05/2001"},
            {"LAchildID": "child2", "CPPID": "cinID2", "CPPreviewDate": "29/05/2001"},
            {"LAchildID": "child3", "CPPID": "cinID3", "CPPreviewDate": "29/05/2004"},
            {"LAchildID": "child4", "CPPID": "cinID4", "CPPreviewDate": "29/05/2004"},
            {"LAchildID": "child5", "CPPID": "cinID5", "CPPreviewDate": "29/05/2004"},
            {"LAchildID": "child6", "CPPID": "cinID6", "CPPreviewDate": "29/05/2004"},
        ]
    )

    # If rule requires columns containing date values, convert those columns to datetime objects first. Do it here in the test_validate function, not above.
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

    # Run the rule function, passing in our sample data.
    result = run_rule(
        validate,
        {
            ChildProtectionPlans: sample_cpp,
            CINplanDates: sample_cin,
            Reviews: sample_reviews,
        },
    )

    # Use .type2_issues to check for the result of .push_type2_issues() which you used above.
    issues_list = result.type2_issues
    assert len(issues_list) == 3
    # the function returns a list on NamedTuples where each NamedTuple contains (table, column_list, df_issues)
    # pick any table and check it's values. the tuple in location 1 will contain the Reviews columns because that's the second thing pushed above.
    issues = issues_list[2]

    # get table name and check it. Replace Reviews with the name of your table.
    issue_table = issues.table
    assert issue_table == Reviews

    # check that the right columns were returned. Replace CPPreviewDate  with a list of your columns.
    issue_columns = issues.columns
    assert issue_columns == [CPPreviewDate]

    # check that the location linking dataframe was formed properly.
    issue_rows = issues.row_df
    # replace 3 with the number of failing points you expect from the sample data.
    assert len(issue_rows) == 1
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
                    pd.to_datetime("29/05/2001", format="%d/%m/%Y", errors="coerce"),
                ),
                "ROW_ID": [0],
            }
        ]
    )
    print(expected_df)
    assert issue_rows.equals(expected_df)

    # Check that the rule definition is what you wrote in the context above.

    # replace 2885 with the rule code and put the appropriate message in its place too.
    assert result.definition.code == 4003
    assert (
        result.definition.message
        == "A CPP review date is shown as being held at the same time as an open CIN Plan."
    )
