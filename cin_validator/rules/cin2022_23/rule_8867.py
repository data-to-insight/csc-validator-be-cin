from typing import Mapping

import pandas as pd

from cin_validator.rule_engine import CINTable, RuleContext, rule_definition
from cin_validator.test_engine import run_rule

# Get tables and columns of interest from the CINTable object defined in rule_engine/__api.py

CINdetails = CINTable.CINdetails
CINclosureDate = CINdetails.CINclosureDate
LAchildID = CINdetails.LAchildID
CINdetailsID = CINdetails.CINdetailsID

Assessments = CINTable.Assessments
AssessmentAuthorisationDate = Assessments.AssessmentAuthorisationDate
LAchildID = Assessments.LAchildID
CINdetailsAssID = Assessments.CINdetailsID


# define characteristics of rule
@rule_definition(
    code="8867",
    module=CINTable.CINdetails,
    message="CIN episode is shown as closed, however Assessment is not shown as completed",
    affected_fields=[
        CINclosureDate,
        AssessmentAuthorisationDate,
    ],
)
def validate(
    data_container: Mapping[CINTable, pd.DataFrame], rule_context: RuleContext
):
    # PREPARING DATA

    df_cind = data_container[CINdetails].copy()
    df_ass = data_container[Assessments].copy()

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

    # If <CINclosureDate> (N00102) is present then all instances of the <Assessments> group must include the <AssesssmentAuthorisationDate> (N00160)
    # Issues dfs should return rows where an AssessmentAuthorisationDate exists if a CINclosureDate has been recorded for that child.

    #  Create dataframes which only have rows with a CINclosureDate and an AssessmentAuthorisation Date and which should have one plan per row.
    df_cind = df_cind[df_cind[CINclosureDate].notna()]

    #  Merge tables to get corresponding CP plan group and reviews
    df_merged = df_cind.merge(
        df_ass,
        left_on=["LAchildID", "CINdetailsID"],
        right_on=["LAchildID", "CINdetailsID"],
        how="inner",
        suffixes=("_cind", "_ass"),
    )

    #  Get rows where there is no AssessmentAuthorisationDate when a CINclosureDate is recorded.
    condition = df_merged[AssessmentAuthorisationDate].isna()
    df_merged = df_merged[condition].reset_index()

    # create an identifier for each error instance.
    # In this case, the rule is checked for each CINclosureDate and AssessmentAuthorisationDate in each child (differentiated by LAchildID)
    # So, a combination of LAchildID, CINclosureDate and AssessmentAuthorisationDate identifies and error instance.
    df_merged["ERROR_ID"] = tuple(
        zip(
            df_merged[LAchildID],
            df_merged[CINdetailsID],
            df_merged[CINclosureDate],
        )
    )

    # The merges were done on copies of df_cind and df_ass so that the column names in dataframes themselves aren't affected by the suffixes.
    # we can now map the suffixes columns to their corresponding source tables such that the failing ROW_IDs and ERROR_IDs exist per table.
    df_cind_issues = (
        df_cind.merge(df_merged, left_on="ROW_ID", right_on="ROW_ID_cind")
        .groupby("ERROR_ID", group_keys="False")["ROW_ID"]
        .apply(list)
        .reset_index()
    )

    df_ass_issues = (
        df_ass.merge(df_merged, left_on="ROW_ID", right_on="ROW_ID_ass")
        .groupby("ERROR_ID", group_keys="False")["ROW_ID"]
        .apply(list)
        .reset_index()
    )

    # Ensure that you maintain the ROW_ID, and ERROR_ID column names which are shown above. They are keywords in this project.
    rule_context.push_type_2(
        table=CINdetails, columns=[CINclosureDate], row_df=df_cind_issues
    )
    rule_context.push_type_2(
        table=Assessments, columns=[AssessmentAuthorisationDate], row_df=df_ass_issues
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
                "CINclosureDate": "29/08/2002",  # Fails (no Assessment Authorisation Date entered)
            },
            {
                "LAchildID": "child3",
                "CINdetailsID": "CDID3",
                "CINclosureDate": "25/10/2002",  # Pass
            },
            {
                "LAchildID": "child4",
                "CINdetailsID": "CDID4",
                "CINclosureDate": "10/06/2000",  # Pass
            },
            {
                "LAchildID": "child5",
                "CINdetailsID": "CDID5",
                "CINclosureDate": "15/11/2001",  # Fails (no Assessment Authorisation Date entered)
            },
            {
                "LAchildID": "child6",
                "CINdetailsID": "CDID5",
                "CINclosureDate": "15/11/2001",  # Passes, no Assessment module
            },
        ]
    )

    sample_ass = pd.DataFrame(
        [
            {
                "LAchildID": "child1",
                "CINdetailsID": "CDID1",
                "AssessmentAuthorisationDate": "26/05/2000",  # Pass
            },
            {
                "LAchildID": "child2",
                "CINdetailsID": "CDID2",
                "AssessmentAuthorisationDate": pd.NA,  # Fails (no date entered)
            },
            {
                "LAchildID": "child3",
                "CINdetailsID": "CDID3",
                "AssessmentAuthorisationDate": "26/05/2000",  # Pass
            },
            {
                "LAchildID": "child4",
                "CINdetailsID": "CDID4",
                "AssessmentAuthorisationDate": "30/05/2000",  # Pass
            },
            {
                "LAchildID": "child5",
                "CINdetailsID": "CDID5",
                "AssessmentAuthorisationDate": pd.NA,  # Fails (no date entered)
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
            CINdetails: sample_cind,
            Assessments: sample_ass,
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
    assert issue_columns == [AssessmentAuthorisationDate]

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
                    "child2",  # ChildID
                    "CDID2",  # CindetailsID
                    # CIN Closure Date
                    pd.to_datetime("29/08/2002", format="%d/%m/%Y", errors="coerce"),
                ),
                "ROW_ID": [1],
            },
            {
                "ERROR_ID": (
                    "child5",  # ChildID
                    "CDID5",  # CindetailsID
                    # CIN Closure Date
                    pd.to_datetime("15/11/2001", format="%d/%m/%Y", errors="coerce"),
                ),
                "ROW_ID": [4],
            },
        ]
    )
    assert issue_rows.equals(expected_df)
    assert (issue_rows["ROW_ID"] == expected_df["ROW_ID"]).all()
    assert (issue_rows["ERROR_ID"] == expected_df["ERROR_ID"]).all()

    # Check that the rule definition is what you wrote in the context above.

    assert result.definition.code == "8867"
    assert (
        result.definition.message
        == "CIN episode is shown as closed, however Assessment is not shown as completed"
    )
