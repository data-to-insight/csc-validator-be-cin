from typing import Mapping

import pandas as pd

from cin_validator.rule_engine import CINTable, RuleContext, rule_definition
from cin_validator.test_engine import run_rule
from cin_validator.utils import make_census_period

# Get tables and columns of interest from the CINTable object defined in rule_engine/__api.py

Assessments = CINTable.Assessments
AssessmentActualStartDate = Assessments.AssessmentActualStartDate
LAchildID = Assessments.LAchildID

CINdetails = CINTable.CINdetails
CINreferralDate = CINdetails.CINreferralDate
LAchildID = CINdetails.LAchildID

# define characteristics of rule
@rule_definition(
    code=1103,
    module=CINTable.Assessments,
    message="The assessment start date cannot be before the referral date",
    affected_fields=[
        AssessmentActualStartDate,
        CINreferralDate,
    ],
)
def validate(
    data_container: Mapping[CINTable, pd.DataFrame], rule_context: RuleContext
):
    # PREPARING DATA

    # Replace ChildProtectionPlans with the name of the table you need.
    df_ass = data_container[Assessments].copy()
    df_referrals = data_container[CINdetails].copy()

    # Before you begin, rename the index so that the initial row positions can be kept intact.
    df_ass.index.name = "ROW_ID"
    df_referrals.index.name = "ROW_ID"

    # Resetting the index causes the ROW_IDs to become columns of their respective DataFrames
    # so that they can come along when the merge is done.
    df_ass.reset_index(inplace=True)
    df_referrals.reset_index(inplace=True)

    # lOGIC
    # Implement rule logic as described by the Github issue.
    # Put the description as a comment above the implementation as shown.

    # Within a <ChildProtectionPlans> group, there should be no <CPPreviewDate> (N00116) that is the same as or before the <CPPstartDate> (N00105)
    # Issues dfs should return rows where CPPreviewDate is less than or equal to the CPPstartDate

    #  Create dataframes which only have rows with CP plans, and which should have one plan per row.
    df_ass = df_ass[df_ass[AssessmentActualStartDate].notna()]
    df_referrals = df_referrals[df_referrals[CINreferralDate].notna()]

    #  Merge tables to get corresponding CP plan group and reviews
    df_merged = df_ass.merge(
        df_referrals,
        left_on=["LAchildID"],
        right_on=["LAchildID"],
        how="left",
        suffixes=("_ass", "_referrals"),
    )

    #  Get rows where CPPreviewDate is less than or equal to CPPstartDate
    condition = df_merged[AssessmentActualStartDate] > 
    df_merged = df_merged[condition].reset_index()

    # create an identifier for each error instance.
    # In this case, the rule is checked for each CPPstartDate, in each CPplanDates group (differentiated by CP dates), in each child (differentiated by LAchildID)
    # So, a combination of LAchildID, CPPstartDate and CPPreviewDate identifies and error instance.
    df_merged["ERROR_ID"] = tuple(
        zip(df_merged[LAchildID], df_merged[AssessmentActualStartDate], df_merged[CINreferralDate])
    )

    # The merges were done on copies of cpp_df and reviews_df so that the column names in dataframes themselves aren't affected by the suffixes.
    # we can now map the suffixes columns to their corresponding source tables such that the failing ROW_IDs and ERROR_IDs exist per table.
    df_ass_issues = (
        df_ass.merge(df_merged, left_on="ROW_ID", right_on="ROW_ID_ass")
        .groupby("ERROR_ID")["ROW_ID"]
        .apply(list)
        .reset_index()
    )
    df_referrals_issues = (
        df_referrals.merge(df_merged, left_on="ROW_ID", right_on="ROW_ID_referrals")
        .groupby("ERROR_ID")["ROW_ID"]
        .apply(list)
        .reset_index()
    )

    # Ensure that you maintain the ROW_ID, and ERROR_ID column names which are shown above. They are keywords in this project.
    rule_context.push_type_2(
        table=Assessments, columns=[AssessmentActualStartDate], row_df=df_ass_issues
    )
    rule_context.push_type_2(
        table=CINdetails, columns=[CINreferralDate], row_df=df_referrals_issues
    )


def test_validate():
    # Create some sample data such that some values pass the validation and some fail.
    sample_ass = pd.DataFrame(
        [
            {
                "LAchildID": "child1",
                "AssessmentActualStartDate": "30/06/2021",  # Fails as referral date is after assessment start
            },
            {
                "LAchildID": "child2",
                "AssessmentActualStartDate": "10/09/2021", #  Passes as assessment starts after referal
            },
            {
                "LAchildID": "child3",
                "AssessmentActualStartDate": pd.NA,   # Fails 
            },
            {
                "LAchildID": "child4",
                "AssessmentActualStartDate": "01/12/2021", # Faisl as assessment starts after referral
            },
            {
                "LAchildID": "child5",
                "AssessmentActualStartDate": "10/02/2022", # Fails
            },
        ]
    )
    sample_referrals = pd.DataFrame(
        [
            {
                "LAchildID": "child1",  #Fails
                "CINreferralDate": "01/07/2021",
            },
            {
                "LAchildID": "child2",  #Passes
                "CINreferralDate": "01/09/2021",
            },
            {
                "LAchildID": "child3",  #Fails
                "CINreferralDate": "26/05/2000",
            },
            {
                "LAchildID": "child4",
                "CINreferralDate": "10/12/2021",
            },
            {
                "LAchildID": "child5", # Fails
                "CINreferralDate": pd.NA,
            },
        ]
    )

    # If rule requires columns containing date values, convert those columns to datetime objects first. Do it here in the test_validate function, not above.
    sample_ass[AssessmentActualStartDate] = pd.to_datetime(
        sample_ass[AssessmentActualStartDate], format="%d/%m/%Y", errors="coerce"
    )
    sample_referrals[CINreferralDate] = pd.to_datetime(
        sample_referrals[CINreferralDate], format="%d/%m/%Y", errors="coerce"
    )

    # Run the rule function, passing in our sample data.
    result = run_rule(
        validate,
        {
            Assessments: sample_ass,
            CINdetails: sample_referrals,
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
    assert issue_table == Assessments

    # check that the right columns were returned. Replace CPPreviewDate  with a list of your columns.
    issue_columns = issues.columns
    assert issue_columns == [AssessmentActualStartDate]

    # check that the location linking dataframe was formed properly.
    issue_rows = issues.row_df
    # replace 3 with the number of failing points you expect from the sample data.
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
                    "child1",  # ChildID
                    # Assessment Date
                    pd.to_datetime("30/06/2021", format="%d/%m/%Y", errors="coerce"),
                    # Referral date
                    pd.to_datetime("01/07/2021", format="%d/%m/%Y", errors="coerce"),
                ),
                "ROW_ID": [0],
            },
            {
                "ERROR_ID": (
                    "child4",  # ChildID
                    # Assessmwent date
                    pd.to_datetime("01/12/2021", format="%d/%m/%Y", errors="coerce"),
                    # Referral date
                    pd.to_datetime("10/12/2021", format="%d/%m/%Y", errors="coerce"),
                ),
                "ROW_ID": [3],
            },
        ]
    )
    assert issue_rows.equals(expected_df)

    # Check that the rule definition is what you wrote in the context above.

    # replace 2885 with the rule code and put the appropriate message in its place too.
    assert result.definition.code == 1103
    assert (
        result.definition.message
        == "The assessment start date cannot be before the referral date"
    )
