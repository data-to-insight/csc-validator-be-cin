from typing import Mapping

import pandas as pd

from cin_validator.rule_engine import CINTable, RuleContext, rule_definition
from cin_validator.test_engine import run_rule

# Get tables and columns of interest from the CINTable object defined in rule_engine/__api.py

ChildProtectionPlans = CINTable.ChildProtectionPlans
CPPstartDate = ChildProtectionPlans.CPPstartDate
LAchildID = ChildProtectionPlans.LAchildID
CPPID_CPP = ChildProtectionPlans.CPPID
CINdetailsCPP = ChildProtectionPlans.CINdetailsID

Reviews = CINTable.Reviews
CPPreviewDate = Reviews.CPPreviewDate
CPPID_reviews = Reviews.CPPID
CINdetailsReviews = Reviews.CINdetailsID


# define characteristics of rule
@rule_definition(
    # write the rule code here
    code="8841",
    # replace ChildProtectionPlans with the value in the module column of the excel sheet corresponding to this rule .
    # Note that even if multiple tables are involved, one table will be named in the module column.
    module=CINTable.ChildProtectionPlans,
    # replace the message with the corresponding value for this rule, gotten from the excel sheet.
    message="The review date cannot be on the same day or before the Child protection Plan start date.",
    # The column names tend to be the words within the < > signs in the github issue description.
    affected_fields=[
        CPPstartDate,
        CPPreviewDate,
    ],
)
def validate(
    data_container: Mapping[CINTable, pd.DataFrame], rule_context: RuleContext
):
    # PREPARING DATA

    df_cpp = data_container[ChildProtectionPlans].copy()
    df_reviews = data_container[Reviews].copy()

    # Before you begin, rename the index so that the initial row positions can be kept intact.
    df_cpp.index.name = "ROW_ID"
    df_reviews.index.name = "ROW_ID"

    # Resetting the index causes the ROW_IDs to become columns of their respective DataFrames
    # so that they can come along when the merge is done.
    df_cpp.reset_index(inplace=True)
    df_reviews.reset_index(inplace=True)

    # lOGIC
    # Implement rule logic as described by the Github issue.
    # Put the description as a comment above the implementation as shown.

    # Within a <ChildProtectionPlans> group, there should be no <CPPreviewDate> (N00116) that is the same as or before the <CPPstartDate> (N00105)
    # Issues dfs should return rows where CPPreviewDate is less than or equal to the CPPstartDate

    #  Create dataframes which only have rows with CP plans, and which should have one plan per row.
    df_cpp = df_cpp[df_cpp[CPPstartDate].notna()]
    df_reviews = df_reviews[df_reviews[CPPreviewDate].notna()]

    #  Merge tables to get corresponding CP plan group and reviews
    df_merged = df_cpp.merge(
        df_reviews,
        left_on=["CPPID", "LAchildID", "CINdetailsID"],
        right_on=["CPPID", "LAchildID", "CINdetailsID"],
        how="left",
        suffixes=("_cpp", "_reviews"),
    )

    #  Get rows where CPPreviewDate is less than or equal to CPPstartDate
    condition = df_merged[CPPreviewDate] <= df_merged[CPPstartDate]
    df_merged = df_merged[condition].reset_index()

    # create an identifier for each error instance.
    # In this case, the rule is checked for each CPPstartDate, in each CPplanDates group (differentiated by CP dates), in each child (differentiated by LAchildID)
    # So, a combination of LAchildID, CPPstartDate and CPPreviewDate identifies and error instance.
    df_merged["ERROR_ID"] = tuple(
        zip(df_merged[LAchildID], df_merged[CPPstartDate], df_merged[CPPreviewDate])
    )

    # The merges were done on copies of cpp_df and reviews_df so that the column names in dataframes themselves aren't affected by the suffixes.
    # we can now map the suffixes columns to their corresponding source tables such that the failing ROW_IDs and ERROR_IDs exist per table.
    df_cpp_issues = (
        df_cpp.merge(df_merged, left_on="ROW_ID", right_on="ROW_ID_cpp")
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

    # Ensure that you maintain the ROW_ID, and ERROR_ID column names which are shown above. They are keywords in this project.
    rule_context.push_type_2(
        table=ChildProtectionPlans, columns=[CPPstartDate], row_df=df_cpp_issues
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
                "CINdetailsID": "CDID1",
                "CPPstartDate": "26/05/2000",  # Fails as dates are the same
                "CPPID": "cinID1",
            },
            {
                "LAchildID": "child1",
                "CINdetailsID": "CDID2",
                "CPPstartDate": "27/06/2002",  #  Fails, review (26/5/2000) before start
                "CPPID": "cinID2",
            },
            {
                "LAchildID": "child3",
                "CINdetailsID": "CDID6",
                "CPPstartDate": "07/02/2001",  # Fails as review is before start (26/5/2000)
                "CPPID": "cinID6",
            },
            {
                "LAchildID": "child2",
                "CINdetailsID": "CDID3",
                "CPPstartDate": "26/05/2000",  # Passes as Start is before Review (30/05/2000)
                "CPPID": "cinID3",
            },
            {
                "LAchildID": "child3",
                "CINdetailsID": "CDID4",
                "CPPstartDate": "26/05/2000",  # Passes
                "CPPID": "cinID4",
            },
            {
                "LAchildID": "child3",
                "CINdetailsID": "CDID5",
                "CPPstartDate": pd.NA,  # Ignored as rows with no start and end are dropped (this is picked up by other rules)
                "CPPID": "cinID5",
            },
            {
                "LAchildID": "child3",
                "CINdetailsID": "CDID7",
                "CPPstartDate": "14/03/2001",  # Ignored as there is no review date
                "CPPID": "cinID7",
            },
        ]
    )
    sample_reviews = pd.DataFrame(
        [
            {
                "LAchildID": "child1",  # Fails
                "CINdetailsID": "CDID1",
                "CPPreviewDate": "26/05/2000",
                "CPPID": "cinID1",
            },
            {
                "LAchildID": "child1",  # Fails
                "CINdetailsID": "CDID2",
                "CPPreviewDate": "26/05/2000",
                "CPPID": "cinID2",
            },
            {
                "LAchildID": "child3",  # Fails
                "CINdetailsID": "CDID6",
                "CPPreviewDate": "26/05/2000",
                "CPPID": "cinID6",
            },
            {
                "LAchildID": "child2",
                "CINdetailsID": "CDID3",
                "CPPreviewDate": "30/05/2000",
                "CPPID": "cinID3",
            },
            {
                "LAchildID": "child3",
                "CINdetailsID": "CDID4",
                "CPPreviewDate": "27/05/2000",
                "CPPID": "cinID4",
            },
            {
                "LAchildID": "child3",
                "CINdetailsID": "CDID5",
                "CPPreviewDate": "26/05/2000",
                "CPPID": "cinID5",
            },
            {
                "LAchildID": "child3",
                "CINdetailsID": "CDID7",
                "CPPreviewDate": pd.NA,
                "CPPID": "cinID7",
            },
        ]
    )

    # If rule requires columns containing date values, convert those columns to datetime objects first. Do it here in the test_validate function, not above.
    sample_cpp[CPPstartDate] = pd.to_datetime(
        sample_cpp[CPPstartDate], format="%d/%m/%Y", errors="coerce"
    )
    sample_reviews["CPPreviewDate"] = pd.to_datetime(
        sample_reviews["CPPreviewDate"], format="%d/%m/%Y", errors="coerce"
    )

    # Run the rule function, passing in our sample data.
    result = run_rule(
        validate,
        {
            ChildProtectionPlans: sample_cpp,
            Reviews: sample_reviews,
        },
    )

    # Use .type2_issues to check for the result of .push_type2_issues() which you used above.
    issues_list = result.type2_issues
    assert len(issues_list) == 2
    # the function returns a list on NamedTuples where each NamedTuple contains (table, column_list, df_issues)
    # pick any table and check it's values. the tuple in location 1 will contain the Reviews columns because that's the second thing pushed above.
    issues = issues_list[1]

    # get table name and check it. Replace Reviews with the name of your table.
    issue_table = issues.table
    assert issue_table == Reviews

    # check that the right columns were returned. Replace CPPreviewDate  with a list of your columns.
    issue_columns = issues.columns
    assert issue_columns == [CPPreviewDate]

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
                    "child1",  # ChildID
                    # Start Date
                    pd.to_datetime("26/05/2000", format="%d/%m/%Y", errors="coerce"),
                    # Review date
                    pd.to_datetime("26/05/2000", format="%d/%m/%Y", errors="coerce"),
                ),
                "ROW_ID": [0],
            },
            {
                "ERROR_ID": (
                    "child1",  # ChildID
                    # Start date
                    pd.to_datetime("27/06/2002", format="%d/%m/%Y", errors="coerce"),
                    # Review date
                    pd.to_datetime("26/05/2000", format="%d/%m/%Y", errors="coerce"),
                ),
                "ROW_ID": [1],
            },
            {
                "ERROR_ID": (
                    "child3",  # ChildID
                    # Start date
                    pd.to_datetime("07/02/2001", format="%d/%m/%Y", errors="coerce"),
                    # Review date
                    pd.to_datetime("26/05/2000", format="%d/%m/%Y", errors="coerce"),
                ),
                "ROW_ID": [2],
            },
        ]
    )
    assert issue_rows.equals(expected_df)

    # Check that the rule definition is what you wrote in the context above.

    # replace '8841' with the rule code and put the appropriate message in its place too.
    assert result.definition.code == "8841"
    assert (
        result.definition.message
        == "The review date cannot be on the same day or before the Child protection Plan start date."
    )
