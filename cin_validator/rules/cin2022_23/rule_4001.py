from typing import Mapping

import pandas as pd

from cin_validator.rule_engine import CINTable, RuleContext, rule_definition
from cin_validator.test_engine import run_rule
from cin_validator.utils import make_census_period

# Get tables and columns of interest from the CINTable object defined in rule_engine/__api.py

ChildProtectionPlans = CINTable.ChildProtectionPlans
LAchildID = ChildProtectionPlans.LAchildID
CINdetailsID = ChildProtectionPlans.CINdetailsID
CPPID = ChildProtectionPlans.CPPID
CPPendDate = ChildProtectionPlans.CPPendDate

CINplanDates = CINTable.CINplanDates
CINPlanEndDate = CINplanDates.CINPlanEndDate


# define characteristics of rule
@rule_definition(
    # write the rule code here
    code=4001,
    # replace ChildProtectionPlans with the value in the module column of the excel sheet corresponding to this rule .
    # Note that even if multiple tables are involved, one table will be named in the module column.
    module=CINTable.CINplanDates,
    # replace the message with the corresponding value for this rule, gotten from the excel sheet.
    message="A CIN Plan cannot run concurrently with a Child Protection Plan",
    # The column names tend to be the words within the < > signs in the github issue description.
    affected_fields=[
        CINPlanEndDate,
        CPPendDate,
    ],
)
def validate(
    data_container: Mapping[CINTable, pd.DataFrame], rule_context: RuleContext
):
    # PREPARING DATA

    # Replace ChildProtectionPlans with the name of the table you need.
    df_cpp = data_container[ChildProtectionPlans].copy()
    df_cin = data_container[CINplanDates].copy()

    # Before you begin, rename the index so that the initial row positions can be kept intact.
    df_cpp.index.name = "ROW_ID"
    df_cin.index.name = "ROW_ID"

    # Resetting the index causes the ROW_IDs to become columns of their respective DataFrames
    # so that they can come along when the merge is done.
    df_cpp.reset_index(inplace=True)
    df_cin.reset_index(inplace=True)

    # lOGIC
    # Implement rule logic as described by the Github issue.
    # Put the description as a comment above the implementation as shown.

    # If a <CINDetails> module has a <ChildProtectionPlan> module present with no <CPPendDate> (N00115)
    # - then a <CINPlanDates> module with no <CINPlanEndDate> (N00690) must not be present

    #  Merge tables
    df_merged = df_cpp.merge(
        df_cin,
        on=["LAchildID", "CINdetailsID"],
        how="left",
        suffixes=("_cpp", "_cin"),
    )

    #  Get rows where CPPendDate is null and CINPlanEndDate is null
    condition = df_merged[CPPendDate].isna() & df_merged[CINPlanEndDate].isna()
    df_merged = df_merged[condition].reset_index()

    # create an identifier for each error instance.
    df_merged["ERROR_ID"] = tuple(zip(df_merged[LAchildID], df_merged[CINdetailsID]))

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
    # Ensure that you maintain the ROW_ID, and ERROR_ID column names which are shown above. They are keywords in this project.
    rule_context.push_type_2(
        table=ChildProtectionPlans,
        columns=[CPPendDate],
        row_df=df_cpp_issues,
    )
    rule_context.push_type_2(
        table=CINplanDates, columns=[CINPlanEndDate], row_df=df_cin_issues
    )


def test_validate():
    # Create some sample data such that some values pass the validation and some fail.
    sample_cpp = pd.DataFrame(
        [
            {
                "LAchildID": "child1",  # 0 Pass
                "CPPendDate": "30/05/2000",
                "CINdetailsID": "cinID1",
            },
            {
                "LAchildID": "child1",  # 1 Pass
                "CPPendDate": pd.NA,
                "CINdetailsID": "cinID1",
            },
            {
                "LAchildID": "child2",  # 2 Fail
                "CPPendDate": pd.NA,
                "CINdetailsID": "cinID3",
            },
            {
                "LAchildID": "child3",  # 3 Pass
                "CPPendDate": "30/10/2001",
                "CINdetailsID": "cinID5",
            },
            {
                "LAchildID": "child5",  # 4 Pass
                "CPPendDate": pd.NA,
                "CINdetailsID": "cinID7",
            },
            {
                "LAchildID": "child6",  # 4 Pass
                "CPPendDate": pd.NA,
                "CINdetailsID": "cinID8",
            },
        ]
    )
    sample_cin = pd.DataFrame(
        [
            {
                "LAchildID": "child1",  # 0 Pass
                "CINPlanEndDate": "04/04/2000",
                "CINdetailsID": "cinID1",
            },
            {
                "LAchildID": "child1",  # 1 Pass
                "CINPlanEndDate": "28/05/2000",
                "CINdetailsID": "cinID1",
            },
            {
                "LAchildID": "child2",  # 2 Fail
                "CINPlanEndDate": pd.NA,
                "CINdetailsID": "cinID3",
            },
            {
                "LAchildID": "child2",  # 3 Pass
                "CINPlanEndDate": pd.NA,
                "CINdetailsID": "cinID4",
            },
            {
                "LAchildID": "child3",  # 4 Pass
                "CINPlanEndDate": "30/10/2001",
                "CINdetailsID": "cinID5",
            },
            {
                "LAchildID": "child4",  # 5 Pass
                "CINPlanEndDate": pd.NA,
                "CINdetailsID": "cinID6",
            },
            {
                "LAchildID": "child6",  # 6 Pass
                "CINPlanEndDate": pd.NA,
                "CINdetailsID": "cinID9",
            },
        ]
    )

    # If rule requires columns containing date values, convert those columns to datetime objects first. Do it here in the test_validate function, not above.
    sample_cpp[CPPendDate] = pd.to_datetime(
        sample_cpp[CPPendDate], format="%d/%m/%Y", errors="coerce"
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
    assert issue_table == CINplanDates

    # check that the right columns were returned. Replace CPPreviewDate  with a list of your columns.
    issue_columns = issues.columns
    assert issue_columns == [CINPlanEndDate]

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
                    "child2",
                    "cinID3",
                ),
                "ROW_ID": [2],
            },
        ]
    )
    assert issue_rows.equals(expected_df)

    # Check that the rule definition is what you wrote in the context above.

    # replace 2885 with the rule code and put the appropriate message in its place too.
    assert result.definition.code == 4001
    assert (
        result.definition.message
        == "A CIN Plan cannot run concurrently with a Child Protection Plan"
    )
