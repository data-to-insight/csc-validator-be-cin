from typing import Mapping

import pandas as pd

from cin_validator.rule_engine import CINTable, RuleContext, RuleType, rule_definition
from cin_validator.test_engine import run_rule

# Get tables and columns of interest from the CINTable object defined in rule_engine/__api.py

CINdetails = CINTable.CINdetails
Assessments = CINTable.Assessments

LAchildID = CINdetails.LAchildID
CINdetailsID = CINdetails.CINdetailsID
ReasonForClosure = CINdetails.ReasonForClosure


# define characteristics of rule
@rule_definition(
    # write the rule code here, in place of 2885
    code="8825Q",
    # replace CINdetails with the value in the module column of the excel sheet corresponding to this rule .
    # Note that even if multiple tables are involved, one table will be named in the module column.
    module=CINTable.CINdetails,
    rule_type=RuleType.QUERY,
    # replace the message with the corresponding value for this rule, gotten from the excel sheet.
    message="Please check and either amend data or provide a reason: Reason for Closure code RC8 (case closed after assessment) or RC9 (case closed after assessment, referred to early help) has been returned but there is no assessment present for the episode.",
    # The column names tend to be the words within the < > signs in the github issue description.
    affected_fields=[
        ReasonForClosure,
    ],
)
def validate(
    data_container: Mapping[CINTable, pd.DataFrame], rule_context: RuleContext
):
    # PREPARING DATA

    df_ass = data_container[Assessments]
    df_cin = data_container[CINdetails]

    # Before you begin, rename the index so that the initial row positions can be kept intact.
    df_ass.index.name = "ROW_ID"
    df_cin.index.name = "ROW_ID"

    # Resetting the index causes the ROW_IDs to become columns of their respective DataFrames
    # so that they can come along when the merge is done.
    df_ass.reset_index(inplace=True)
    df_cin.reset_index(inplace=True)

    # lOGIC
    # If <ReasonforClosure> (N00103) = RC8 or RC9 then at least one <Assessments> module must be present

    df_cin_check = df_cin.copy()

    df_cin_check = df_cin_check[df_cin_check[ReasonForClosure].isin(["RC8", "RC9"])]

    merged_df = df_cin_check.merge(
        df_ass,
        on=[LAchildID, CINdetailsID],
        suffixes=["_cin", "_ass"],
        how="left",
        indicator=True,
    )
    # get modules whose ReasonForClosure is RC8/RC9 but are not found in the assessment table.
    condition = merged_df["_merge"] == "left_only"
    merged_df = merged_df[condition].reset_index()

    # create an identifier for each error instance.
    merged_df["ERROR_ID"] = tuple(zip(merged_df[LAchildID], merged_df[CINdetailsID]))

    # The merges were done on copies of df_ass and df_cin so that the column names in dataframes themselves aren't affected by the suffixes.
    # we can now map the suffixes columns to their corresponding source tables such that the failing ROW_IDs and ERROR_IDs exist per table.
    df_ass_issues = (
        df_ass.merge(merged_df, left_on="ROW_ID", right_on="ROW_ID_ass")
        .groupby("ERROR_ID", group_keys=False)["ROW_ID"]
        .apply(list)
        .reset_index()
    )
    df_cin_issues = (
        df_cin.merge(merged_df, left_on="ROW_ID", right_on="ROW_ID_cin")
        .groupby("ERROR_ID", group_keys=False)["ROW_ID"]
        .apply(list)
        .reset_index()
    )

    # Ensure that you maintain the ROW_ID, and ERROR_ID column names which are shown above. They are keywords in this project.
    rule_context.push_type_2(
        table=Assessments, columns=[CINdetailsID], row_df=df_ass_issues
    )
    rule_context.push_type_2(
        table=CINdetails, columns=[ReasonForClosure], row_df=df_cin_issues
    )


def test_validate():
    # Create some sample data such that some values pass the validation and some fail.
    sample_ass = pd.DataFrame(
        [
            {
                "LAchildID": "child7",
                "CINdetailsID": "cinID1",
            },
            {
                "LAchildID": "child3",
                "CINdetailsID": "cinID1",
            },
            {
                "LAchildID": "child3",
                "CINdetailsID": "cinID2",
            },
            {
                "LAchildID": "child3",
                "CINdetailsID": "cinID3",
            },
            {
                "LAchildID": "child3",
                "CINdetailsID": "cinID4",
            },
            {
                "LAchildID": "child4",
                "CINdetailsID": "cinID1",
            },
        ]
    )
    sample_cin_details = pd.DataFrame(
        [
            {  # 0 pass
                "LAchildID": "child1",
                "ReasonForClosure": pd.NA,  # 0 ignore
                "CINdetailsID": "cinID1",
            },
            {  # 1 ignore
                "LAchildID": "child1",
                "ReasonForClosure": "EX7",  # 1 ignore
                "CINdetailsID": "cinID2",
            },
            {  # 2
                "LAchildID": "child2",
                "ReasonForClosure": "EX7",  # 2 ignore
                "CINdetailsID": "cinID1",
            },
            {  # 3
                "LAchildID": "child3",
                "ReasonForClosure": "RC8",  # 3 pass. present in assessment table
                "CINdetailsID": "cinID1",
            },
            {  # 4 ignore
                "LAchildID": "child3",
                "ReasonForClosure": "EX7",  # 4 ignore
                "CINdetailsID": "cinID2",
            },
            {  # 5 pass
                "LAchildID": "child3",
                "ReasonForClosure": "RC9",  # 5 pass. present in assessment table
                "CINdetailsID": "cinID3",
            },
            {  # 6 fail
                "LAchildID": "child4",
                "ReasonForClosure": "RC8",  # 6 fail: no assessment recorded.
                "CINdetailsID": "cinID4",
            },
        ]
    )

    # Run the rule function, passing in our sample data.
    result = run_rule(
        validate,
        {
            Assessments: sample_ass,
            CINdetails: sample_cin_details,
        },
    )

    # Use .type2_issues to check for the result of .push_type2_issues() which you used above.
    issues_list = result.type2_issues
    assert len(issues_list) == 2
    # the function returns a list on NamedTuples where each NamedTuple contains (table, column_list, df_issues)
    # pick any table and check it's values. the tuple in location 1 will contain the CINdetails columns because that's the second thing pushed above.
    issues = issues_list[1]

    # get table name and check it. Replace CINdetails with the name of your table.
    issue_table = issues.table
    assert issue_table == CINdetails

    # check that the right columns were returned. Replace ReasonForClosure with a list of your columns.
    issue_columns = issues.columns
    assert issue_columns == [ReasonForClosure]

    # check that the location linking dataframe was formed properly.
    issue_rows = issues.row_df
    # replace 1 with the number of failing points you expect from the sample data.
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
                    "child4",
                    "cinID4",
                ),
                "ROW_ID": [6],
            },
        ]
    )
    assert issue_rows.equals(expected_df)

    # Check that the rule definition is what you wrote in the context above.

    # replace 8825Q with the rule code and put the appropriate message in its place too.
    assert result.definition.code == "8825Q"
    assert (
        result.definition.message
        == "Please check and either amend data or provide a reason: Reason for Closure code RC8 (case closed after assessment) or RC9 (case closed after assessment, referred to early help) has been returned but there is no assessment present for the episode."
    )
