from typing import Mapping

import pandas as pd

from cin_validator.rule_engine import CINTable, RuleContext, rule_definition
from cin_validator.test_engine import run_rule
from cin_validator.utils import make_census_period

# Get tables and columns of interest from the CINTable object defined in rule_engine/__api.py

CINDetails = CINTable.CINdetails
CINclosureDate = CINDetails.CINclosureDate
LAchildID = CINDetails.LAchildID
CINDetailsID = CINDetails.CINdetailsID

Assessment = CINTable.Assessments
AssessmentAuthorisationDate = Assessment.AssessmentAuthorisationDate
LAchildID = Assessment.LAchildID
CINdetailsAssID = Assessment.CINdetailsID

# define characteristics of rule
@rule_definition(
    # write the rule code here
    code=8867,
    module=CINTable.CINdetails,
    message="CIN episode is shown as closed, however Assessment is not shown as completed.",
    # The column names tend to be the words within the < > signs in the github issue description.
    affected_fields=[
        CINclosureDate,
        AssessmentAuthorisationDate,
    ],
)
def validate(
    data_container: Mapping[CINTable, pd.DataFrame], rule_context: RuleContext
):
    # PREPARING DATA

    # Replace ChildProtectionPlans with the name of the table you need.
    df_cind = data_container[CINDetails].copy()
    df_ass = data_container[Assessment].copy()

    # Before you begin, rename the index so that the initial row positions can be kept intact.
    df_cind.index.name = "ROW_ID"
    df_ass.index.name = "ROW_ID"

    # Resetting the index causes the ROW_IDs to become columns of their respective DataFrames
    # so that they can come along when the merge is done.
    df_cind.reset_index(inplace=True)
    df_ass.reset_index(inplace=True)

    # lOGIC
    # Implement rule logic as described by the Github issue.
    # Put the description as a comment above the implementation as shown.

    #If <CINclosureDate> (N00102) is present then all instances of the <Assessments> group must include the <AssesssmentAuthorisationDate> (N00160)
    # Issues dfs should return rows where CPPreviewDate is less than or equal to the CPPstartDate

    #  Create dataframes which only have rows with CP plans, and which should have one plan per row.
    df_cind = df_cind[df_cind[CINclosureDate].notna()]
    df_ass = df_ass[df_ass[AssessmentAuthorisationDate].notna()]

    #  Merge tables to get corresponding CP plan group and reviews
    df_merged = df_cind.merge(
        df_ass,
        left_on=["LAchildID", "CINdetailsID"],
        right_on=["LAchildID", "CINdetailsID"],
        how="inner",
        suffixes=("_cind", "_ass"),
    )

    #  Get rows where CPPreviewDate is less than or equal to CPPstartDate
    condition = df_merged[AssessmentAuthorisationDate].isna() | df_merged[CINclosureDate]> df_merged[AssessmentAuthorisationDate]
    df_merged = df_merged[condition].reset_index()

    # create an identifier for each error instance.
    # In this case, the rule is checked for each CPPstartDate, in each CPplanDates group (differentiated by CP dates), in each child (differentiated by LAchildID)
    # So, a combination of LAchildID, CPPstartDate and CPPreviewDate identifies and error instance.
    df_merged["ERROR_ID"] = tuple(
        zip(df_merged[LAchildID], df_merged[CINclosureDate], df_merged[AssessmentAuthorisationDate])
    )

    # The merges were done on copies of cpp_df and reviews_df so that the column names in dataframes themselves aren't affected by the suffixes.
    # we can now map the suffixes columns to their corresponding source tables such that the failing ROW_IDs and ERROR_IDs exist per table.
    df_cind_issues = (
        df_cind.merge(df_merged, left_on="ROW_ID", right_on="ROW_ID_cind")
        .groupby("ERROR_ID")["ROW_ID"]
        .apply(list)
        .reset_index()
    )
    df_ass_issues = (
        df_ass.merge(df_merged, left_on="ROW_ID", right_on="ROW_ID_ass")
        .groupby("ERROR_ID")["ROW_ID"]
        .apply(list)
        .reset_index()
    )

    # Ensure that you maintain the ROW_ID, and ERROR_ID column names which are shown above. They are keywords in this project.
    rule_context.push_type_2(
        table=CINDetails, columns=[CINclosureDate], row_df=df_cind_issues
    )
    rule_context.push_type_2(
        table=Assessment, columns=[AssessmentAuthorisationDate], row_df=df_ass_issues
    )


def test_validate():
    # Create some sample data such that some values pass the validation and some fail.
    sample_cind = pd.DataFrame(
        [
            {
                "LAchildID": "child1",
                "CINdetailsID": "CDID1",
                "CINclosureDate": "30/06/2000",  # Pass
            },
            {
                "LAchildID": "child2",
                "CINdetailsID": "CDID2",
                "CINclosureDate": "29/08/2002",  #  Fails
            },
            {
                "LAchildID": "child3",
                "CINdetailsID": "CDID3",
                "CINclosureDate": "25/10/2002",  # Passes
            },
            {
                "LAchildID": "child4",
                "CINdetailsID": "CDID4",
                "CINclosureDate": pd.NA,  # Ignored 
            },
            {
                "LAchildID": "child5",
                "CINdetailsID": "CDID5",
                "CINclosureDate": "15/11/2001",  
            },
        ]
    )
    sample_ass = pd.DataFrame(
        [
            {
                "LAchildID": "child1",  # Pass
                "CINdetailsID": "CDID1",
                "AssessmentAuthorisationDate": "26/05/2000",
            },
            {
                "LAchildID": "child2",  # Fails
                "CINdetailsID": "CDID2",
                "AssessmentAuthorisationDate": "26/10/2002",
            },
            {
                "LAchildID": "child3",  # Passes
                "CINdetailsID": "CDID3",
                "AssessmentAuthorisationDate": "26/05/2000",
            },
            {
                "LAchildID": "child4",
                "CINdetailsID": "CDID4",
                "AssessmentAuthorisationDate": "30/05/2000",
            },
            {
                "LAchildID": "child5",
                "CINdetailsID": "CDID5",
                "AssessmentAuthorisationDate": pd.NA, #Ignored
            },
        ]
    )

    # If rule requires columns containing date values, convert those columns to datetime objects first. Do it here in the test_validate function, not above.
    sample_cind[CINclosureDate] = pd.to_datetime(
        sample_cind[CINclosureDate], format="%d/%m/%Y", errors="coerce"
    )
    sample_ass[AssessmentAuthorisationDate] = pd.to_datetime(
        sample_ass[AssessmentAuthorisationDate], format="%d/%m/%Y", errors="coerce"
    )

    # Run the rule function, passing in our sample data.
    result = run_rule(
        validate,
        {
            CINDetails: sample_cind,
            Assessment: sample_ass,
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
    assert issue_table == Assessment

    # check that the right columns were returned. Replace CPPreviewDate  with a list of your columns.
    issue_columns = issues.columns
    assert issue_columns == [AssessmentAuthorisationDate]

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
                    "child2",  # ChildID
                    # Start Date
                    pd.to_datetime("29/08/2002", format="%d/%m/%Y", errors="coerce"),
                    # Review date
                    pd.to_datetime("26/10/2002", format="%d/%m/%Y", errors="coerce"),
                ),
                "ROW_ID": [1],
            },
        ]
    )
    assert issue_rows.equals(expected_df)

    # Check that the rule definition is what you wrote in the context above.

    # replace 2885 with the rule code and put the appropriate message in its place too.
    assert result.definition.code == 8867
    assert (
        result.definition.message
        == "CIN episode is shown as closed, however Assessment is not shown as completed."
    )
