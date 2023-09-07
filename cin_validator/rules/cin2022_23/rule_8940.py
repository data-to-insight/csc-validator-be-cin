from typing import Mapping

import pandas as pd

from cin_validator.rule_engine import CINTable, RuleContext, rule_definition
from cin_validator.test_engine import run_rule
from cin_validator.utils import make_census_period

# Get tables and columns of interest from the CINTable object defined in rule_engine/__api.py

ChildProtectionPlans = CINTable.ChildProtectionPlans
LAchildID = ChildProtectionPlans.LAchildID
CPPID = ChildProtectionPlans.CPPID
CPPstartDate = ChildProtectionPlans.CPPstartDate
CPPendDate = ChildProtectionPlans.CPPendDate

Header = CINTable.Header
ReferenceDate = Header.ReferenceDate


# define characteristics of rule
@rule_definition(
    # write the rule code here
    code="8940",
    # replace ChildProtectionPlans with the value in the module column of the excel sheet corresponding to this rule .
    # Note that even if multiple tables are involved, one table will be named in the module column.
    module=CINTable.ChildProtectionPlans,
    # replace the message with the corresponding value for this rule, gotten from the excel sheet.
    message="Child Protection Plan data contains overlapping dates",
    # The column names tend to be the words within the < > signs in the github issue description.
    affected_fields=[
        CPPstartDate,
        CPPendDate,
    ],
)
def validate(
    data_container: Mapping[CINTable, pd.DataFrame], rule_context: RuleContext
):
    # PREPARING DATA

    # Replace ChildProtectionPlans with the name of the table you need.
    df_cpp = data_container[ChildProtectionPlans].copy()
    df_cpp2 = data_container[ChildProtectionPlans].copy()

    # Before you begin, rename the index so that the initial row positions can be kept intact.
    df_cpp.index.name = "ROW_ID"
    df_cpp2.index.name = "ROW_ID"

    # Resetting the index causes the ROW_IDs to become columns of their respective DataFrames
    # so that they can come along when the merge is done.
    df_cpp.reset_index(inplace=True)
    df_cpp2.reset_index(inplace=True)

    # ReferenceDate exists in the header table so we get header table too.
    df_ref = data_container[Header]
    ref_date_series = df_ref[ReferenceDate]

    # the make_census_period function generates the start and end date so that you don't have to do it each time.
    collection_start, reference_date = make_census_period(ref_date_series)

    # lOGIC
    # Implement rule logic as described by the Github issue.
    # Put the description as a comment above the implementation as shown.

    # Where more than one <ChildProtectionPlans> group is included,
    # the <CPPstartDate> (N00105) of each group cannot fall within either:
    #   a) <CPPstartDate> (N00105) to <CPPendDate> (N00115), or
    #   b) <CPPstartDate> (N00105) and <ReferenceDate> if <CPPendDate> (N00115) is not present
    # of any other group
    #
    # Issues dfs should return rows where CPPstartDate is between another CPPstartDate and CPPendDate (or ReferenceDate)

    #  Create dataframes which only have rows with CP plans, and which should have one plan per row.
    df_cpp = df_cpp[df_cpp[CPPstartDate].notna()]
    df_cpp2 = df_cpp2[df_cpp2[CPPstartDate].notna()]

    #  Merge tables to test for overlaps
    df_merged = df_cpp.merge(
        df_cpp2,
        on=["LAchildID"],
        how="left",
        suffixes=("_cpp", "_cpp2"),
    )

    # Exclude rows where the CPPID is the same on both sides
    df_merged = df_merged[(df_merged["CPPID_cpp"] != df_merged["CPPID_cpp2"])]

    # Determine whether CPP overlaps another CPP
    cpp_started_after_start = (
        df_merged["CPPstartDate_cpp"] >= df_merged["CPPstartDate_cpp2"]
    )
    cpp_started_before_end = (
        df_merged["CPPstartDate_cpp"] <= df_merged["CPPendDate_cpp2"]
    ) & df_merged["CPPendDate_cpp2"].notna()
    cpp_started_before_refdate = (
        df_merged["CPPstartDate_cpp"] <= reference_date
    ) & df_merged["CPPendDate_cpp2"].isna()

    df_merged = df_merged[
        cpp_started_after_start & (cpp_started_before_end | cpp_started_before_refdate)
    ].reset_index()

    # create an identifier for each error instance.
    # In this case, the rule is checked for each CPPstartDate, in each CPplanDates group (differentiated by CP dates), in each child (differentiated by LAchildID)
    df_merged["ERROR_ID"] = tuple(
        zip(df_merged[LAchildID], df_merged["CPPID_cpp"], df_merged["CPPID_cpp2"])
    )

    # The merges were done on copies of cpp_df so that the column names in dataframes themselves aren't affected by the suffixes.
    # we can now map the suffixes columns to their corresponding source tables such that the failing ROW_IDs and ERROR_IDs exist per table.
    df_cpp_issues = (
        df_cpp.merge(df_merged, left_on="ROW_ID", right_on="ROW_ID_cpp")
        .groupby("ERROR_ID", group_keys=False)["ROW_ID"]
        .apply(list)
        .reset_index()
    )
    df_cpp2_issues = (
        df_cpp2.merge(df_merged, left_on="ROW_ID", right_on="ROW_ID_cpp2")
        .groupby("ERROR_ID", group_keys=False)["ROW_ID"]
        .apply(list)
        .reset_index()
    )

    # Ensure that you maintain the ROW_ID, and ERROR_ID column names which are shown above. They are keywords in this project.
    rule_context.push_type_3(
        table=ChildProtectionPlans, columns=[CPPstartDate], row_df=df_cpp_issues
    )
    rule_context.push_type_3(
        table=ChildProtectionPlans,
        columns=[CPPstartDate, CPPendDate],
        row_df=df_cpp2_issues,
    )


def test_validate():
    # Create some sample data such that some values pass the validation and some fail.
    sample_header = pd.DataFrame(
        [{ReferenceDate: "31/03/2001"}]  # the census start date here will be 01/04/2000
    )

    sample_cpp = pd.DataFrame(
        [
            {
                "LAchildID": "child1",  # 0 Pass
                "CPPstartDate": "26/05/2000",
                "CPPendDate": "26/10/2000",
                "CPPID": "cinID1",
            },
            {
                "LAchildID": "child1",  # 1 Fail
                "CPPstartDate": "26/08/2000",
                "CPPendDate": "26/12/2000",
                "CPPID": "cinID12",
            },
            {
                "LAchildID": "child2",  # 2 Pass
                "CPPstartDate": "26/05/2000",
                "CPPendDate": "25/10/2000",
                "CPPID": "cinID2",
            },
            {
                "LAchildID": "child2",  # 3 Pass
                "CPPstartDate": "26/10/2000",
                "CPPendDate": "26/12/2000",
                "CPPID": "cinID22",
            },
            {
                "LAchildID": "child3",  # 4 Pass
                "CPPstartDate": "26/05/2000",
                "CPPendDate": pd.NA,
                "CPPID": "cinID3",
            },
            {
                "LAchildID": "child3",  # 5 Fail
                "CPPstartDate": "26/08/2000",
                "CPPendDate": "26/10/2000",
                "CPPID": "cinID32",
            },
            {
                "LAchildID": "child4",  # 6 Pass
                "CPPstartDate": "26/10/2000",
                "CPPendDate": "31/03/2001",
                "CPPID": "cinID4",
            },
            {
                "LAchildID": "child4",  # 7 Fail
                "CPPstartDate": "31/03/2001",
                "CPPendDate": pd.NA,
                "CPPID": "cinID42",
            },
        ]
    )

    # If rule requires columns containing date values, convert those columns to datetime objects first. Do it here in the test_validate function, not above.
    sample_cpp[CPPstartDate] = pd.to_datetime(
        sample_cpp[CPPstartDate], format="%d/%m/%Y", errors="coerce"
    )
    sample_cpp["CPPendDate"] = pd.to_datetime(
        sample_cpp["CPPendDate"], format="%d/%m/%Y", errors="coerce"
    )
    sample_header[ReferenceDate] = pd.to_datetime(
        sample_header[ReferenceDate], format="%d/%m/%Y", errors="coerce"
    )

    # Run the rule function, passing in our sample data.
    result = run_rule(
        validate,
        {
            ChildProtectionPlans: sample_cpp,
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
    assert issue_table == ChildProtectionPlans

    # check that the right columns were returned. Replace CPPreviewDate  with a list of your columns.
    issue_columns = issues.columns
    assert issue_columns == [CPPstartDate]

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
                    "cinID12",
                    "cinID1",
                ),
                "ROW_ID": [1],
            },
            {
                "ERROR_ID": (
                    "child3",
                    "cinID32",
                    "cinID3",
                ),
                "ROW_ID": [5],
            },
            {
                "ERROR_ID": (
                    "child4",
                    "cinID42",
                    "cinID4",
                ),
                "ROW_ID": [7],
            },
        ]
    )
    assert issue_rows.equals(expected_df)

    # Check that the rule definition is what you wrote in the context above.

    # replace 2885 with the rule code and put the appropriate message in its place too.
    assert result.definition.code == "8940"
    assert (
        result.definition.message
        == "Child Protection Plan data contains overlapping dates"
    )
