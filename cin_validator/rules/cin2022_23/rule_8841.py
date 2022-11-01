from typing import Mapping

import pandas as pd

from cin_validator.rule_engine import CINTable, RuleContext, rule_definition
from cin_validator.test_engine import run_rule
from cin_validator.utils import make_census_period

# Get tables and columns of interest from the CINTable object defined in rule_engine/__api.py

ChildProtectionPlans = CINTable.ChildProtectionPlans
CPPstartDate =  ChildProtectionPlans.CPPstartDate
LAchildID = ChildProtectionPlans.LAchildID
CPPID_CPP = ChildProtectionPlans.CPPID
Reviews = CINTable.Reviews
CPPreviewDate = Reviews.CPPreviewDate 
CPPID_reviews = Reviews.CPPID

# define characteristics of rule
@rule_definition(
    # write the rule code here, in place of 2885
    code=8841,
    # replace ChildProtectionPlans with the value in the module column of the excel sheet corresponding to this rule .
    # Note that even if multiple tables are involved, one table will be named in the module column.
    module=CINTable.ChildProtectionPlans,
    # replace the message with the corresponding value for this rule, gotten from the excel sheet.
    message="The review date cannot be on the same day or before the Child protection Plan start date.",
    # The column names tend to be the words within the < > signs in the github issue description.
    affected_fields=[
        CPPstartDate,
        CPPreviewDate,
    ],  # TODO How can we indicate that the DateOfInitialCPC comes from both tables. Is it necessary?
)
def validate(
    data_container: Mapping[CINTable, pd.DataFrame], rule_context: RuleContext
):
    # PREPARING DATA

    # Replace ChildProtectionPlans with the name of the table you need.
    df_cpp = data_container[ChildProtectionPlans].copy()
    df_reviews = data_container[Reviews].copy()



    # Before you begin, rename the index so that the initial row positions can be kept intact.
    df_cpp.index.name = "ROW_ID"
    df_reviews.index.name = "ROW_ID"

    # Resetting the index causes the ROW_IDs to become columns of their respective DataFrames
    # so that they can come along when the merge is done.
    # TODO summarise with a for loop? e.g for df in [df_cpp, df_47, df_cin]
    df_cpp.reset_index(inplace=True)
    df_reviews.reset_index(inplace=True)

    # lOGIC
    # Implement rule logic as described by the Github issue.
    # Put the description as a comment above the implementation as shown.

    # Within a <ChildProtectionPlans> group, there should be no <CPPreviewDate> (N00116) that is the same as or before the <CPPstartDate> (N00105)
    # Issues dfs should return rows where CPPreviewDate is less than or equal to the CPPstartDate
    
    #  Create dataframes which only have rows with CP plans, and which should have one plan per row.
    df_cpp = df_cpp[CPPID_CPP].notna()
    df_reviews = df_reviews[CPPID_reviews].notna()

    #  Merge tables to get corresponding CP plan group and reviews
    df_merged = df_cpp.merge(df_reviews, left_on='CPPID_CPP', right_on='CPPID_reviews', how='inner', suffixes=('_cpp', '_reviews'))
    df_merged.drop('CPPID_CPP')
    df_merged.rename(columns={'CPPID_reviews':'CPPID'})

    #  Get rows where CPPreviewDate is less than or equal to CPPstartDate
    condition = df_merged[CPPreviewDate] <= df_merged[CPPstartDate]
    df_merged = df_merged[condition].reset_index()

    # create an identifier for each error instance.
    # In this case, the rule is checked for each CPPstartDate, in each CINplanDates group (differentiated by CINdetailsID), in each child (differentiated by LAchildID)
    # So, a combination of LAchildID, CINdetailsID and CPPstartDate identifies and error instance.
    # You could also consider that CPPstartDate, unlike DateOfInitialCPC, is the leading column against which columns from the other tables are compared. So it is included in the zip.
    df_merged["ERROR_ID"] = tuple(
        zip(df_merged[LAchildID], df_merged[CPPreviewDate], df_merged[CPPstartDate])
    )

    # The merges were done on copies of df_cpp, df_47 and df_cin so that the column names in dataframes themselves aren't affected by the suffixes.
    # we can now map the suffixes columns to their corresponding source tables such that the failing ROW_IDs and ERROR_IDs exist per table.
    df_cpp_issues = (
        df_cpp.merge(df_merged, left_on="ROW_ID", right_on="ROW_ID_cpp")
        .groupby("ERROR_ID")["ROW_ID"]
        .apply(list)
        .reset_index()
    )
    df_reviews_issues = (
        df_reviews.merge(df_merged, left_on="ROW_ID", right_on="ROW_ID_reviews")
        .groupby("ERROR_ID")["ROW_ID"]
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
            {  # same as Section47 date, different from cin date
                "LAchildID": "child1",
                "CPPstartDate": "26/05/2000",  # 0 pass
                "CINdetailsID": "cinID1",
            },
            {  # would've failed but ignored. Not in period of census
                "LAchildID": "child1",
                "CPPstartDate": "27/06/2002",  # 1 ignored
                "CINdetailsID": "cinID2",
            },
            {  # same as cin_date, different from section47
                "LAchildID": "child2",
                "CPPstartDate": "26/05/2000",  # 2 pass [Should fail if other condition is used and section47 is present]
                "CINdetailsID": "cinID1",
            },
            {  # different from both dates
                "LAchildID": "child3",
                "CPPstartDate": "26/05/2000",  # 3 fail
                "CINdetailsID": "cinID1",
            },
            {  # absent
                "LAchildID": "child3",
                "CPPstartDate": pd.NA,  # 4 ignore
                "CINdetailsID": "cinID2",
            },
            {  # fail
                "LAchildID": "child3",
                "CPPstartDate": "07/02/2001",  # 5 fail. Different from both cin_dates in its cindetails group
                "CINdetailsID": "cinID3",
            },
            {  # section47 date is absent, same as cin date.
                # If grouping is not done well, this date could cause (LAchildID3, CINdetailsID3) above to pass.
                "LAchildID": "child3",
                "CPPstartDate": "14/03/2001",  # 6 pass [Should fail if other condition is used]
                "CINdetailsID": "cinID4",
            },
        ]
    )
    sample_section47 = pd.DataFrame(
        [
            {  # 0 pass
                "LAchildID": "child1",
                "DateOfInitialCPC": "26/05/2000",
                "CINdetailsID": "cinID1",
            },
            {  # 1 ignored
                "LAchildID": "child1",
                "DateOfInitialCPC": "26/05/2000",
                "CINdetailsID": "cinID2",
            },
            {  # 2 pass
                "LAchildID": "child2",
                "DateOfInitialCPC": "30/05/2000",
                "CINdetailsID": "cinID1",
            },
            {  # 3 fail
                "LAchildID": "child3",
                "DateOfInitialCPC": "27/05/2000",
                "CINdetailsID": "cinID1",
            },
            {  # 4 absent, ignored
                "LAchildID": "child3",
                "DateOfInitialCPC": "26/05/2000",
                "CINdetailsID": "cinID2",
            },
            {  # 5 fail
                "LAchildID": "child3",
                "DateOfInitialCPC": "26/05/2000",
                "CINdetailsID": "cinID3",
            },
            {  # 6 pass
                "LAchildID": "child3",
                "DateOfInitialCPC": pd.NA,
                "CINdetailsID": "cinID4",
            },
        ]
    )
    sample_cin_details = pd.DataFrame(
        [
            {  # 0 pass
                "LAchildID": "child1",
                "DateOfInitialCPC": "26/10/1999",
                "CINdetailsID": "cinID1",
            },
            {  # 1 ignore
                "LAchildID": "child1",
                "DateOfInitialCPC": "26/05/2000",
                "CINdetailsID": "cinID2",
            },
            {  # 2 pass
                "LAchildID": "child2",
                "DateOfInitialCPC": "26/05/2000",
                "CINdetailsID": "cinID1",
            },
            {  # 3 fail
                "LAchildID": "child3",
                "DateOfInitialCPC": "28/05/2000",
                "CINdetailsID": "cinID1",
            },
            {  # 4 ignore
                "LAchildID": "child3",
                "DateOfInitialCPC": "26/05/2000",
                "CINdetailsID": "cinID2",
            },
            {  # 5 fail
                "LAchildID": "child3",
                "DateOfInitialCPC": "26/05/2003",
                "CINdetailsID": "cinID3",
            },
            {  # 6 pass
                "LAchildID": "child3",
                "DateOfInitialCPC": "14/03/2001",
                "CINdetailsID": "cinID4",
            },
        ]
    )
    # if rule requires columns containing date values, convert those columns to datetime objects first. Do it here in the test_validate function, not above.
    sample_cpp[CPPstartDate] = pd.to_datetime(
        sample_cpp[CPPstartDate], format="%d/%m/%Y", errors="coerce"
    )
    sample_section47["DateOfInitialCPC"] = pd.to_datetime(
        sample_section47["DateOfInitialCPC"], format="%d/%m/%Y", errors="coerce"
    )
    sample_cin_details["DateOfInitialCPC"] = pd.to_datetime(
        sample_cin_details["DateOfInitialCPC"], format="%d/%m/%Y", errors="coerce"
    )
    sample_header = pd.DataFrame(
        [{ReferenceDate: "31/03/2001"}]  # the census start date here will be 01/04/2000
    )

    # Run the rule function, passing in our sample data.
    result = run_rule(
        validate,
        {
            ChildProtectionPlans: sample_cpp,
            Section47: sample_section47,
            CINdetails: sample_cin_details,
            Header: sample_header,
        },
    )

    # Use .type2_issues to check for the result of .push_type2_issues() which you used above.
    issues_list = result.type2_issues
    assert len(issues_list) == 3
    # the function returns a list on NamedTuples where each NamedTuple contains (table, column_list, df_issues)
    # pick any table and check it's values. the tuple in location 1 will contain the Section47 columns because that's the second thing pushed above.
    issues = issues_list[1]

    # get table name and check it. Replace Section47 with the name of your table.
    issue_table = issues.table
    assert issue_table == Section47

    # check that the right columns were returned. Replace DateOfInitialCPC  with a list of your columns.
    issue_columns = issues.columns
    assert issue_columns == [DateOfInitialCPC]

    # check that the location linking dataframe was formed properly.
    issue_rows = issues.row_df
    # replace 2 with the number of failing points you expect from the sample data.
    assert len(issue_rows) == 2
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
                    "child3",  # ChildID
                    "cinID1",  # CINdetailsID
                    # corresponding CPPstartDate
                    pd.to_datetime("26/05/2000", format="%d/%m/%Y", errors="coerce"),
                ),
                "ROW_ID": [3],
            },
            {
                "ERROR_ID": (
                    "child3",
                    "cinID3",
                    pd.to_datetime("07/02/2001", format="%d/%m/%Y", errors="coerce"),
                ),
                "ROW_ID": [5],
            },
        ]
    )
    assert issue_rows.equals(expected_df)

    # Check that the rule definition is what you wrote in the context above.

    # replace 2885 with the rule code and put the appropriate message in its place too.
    assert result.definition.code == 2885
    assert (
        result.definition.message
        == "Child protection plan shown as starting a different day to the initial child protection conference"
    )