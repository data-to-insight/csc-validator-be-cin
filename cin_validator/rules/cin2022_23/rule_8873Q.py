from typing import Mapping

import pandas as pd

from cin_validator.rule_engine import CINTable, RuleContext, RuleType, rule_definition
from cin_validator.test_engine import run_rule

# Get tables and columns of interest from the CINTable object defined in rule_engine/__api.py

Assessments = CINTable.Assessments
LAchildID = Assessments.LAchildID
CINdetailsID = Assessments.CINdetailsID
AssessmentID = Assessments.AssessmentID
AssessmentActualStartDate = Assessments.AssessmentActualStartDate
AssessmentFactors = Assessments.AssessmentFactors

AssessmentFactorsList = CINTable.AssessmentFactorsList
AssessmentFactor = AssessmentFactorsList.AssessmentFactor

CINdetails = CINTable.CINdetails
LAchildID = CINdetails.LAchildID
CINdetailsID_cin = CINdetails.CINdetailsID
ReasonForClosure = CINdetails.ReasonForClosure


# define characteristics of rule
@rule_definition(
    # write the rule code here, in place of 8873Q
    code="8873Q",
    # replace Assesments with the value in the module column of the excel sheet corresponding to this rule .
    # Note that even if multiple tables are involved, one table will be named in the module column.
    module=CINTable.Assessments,
    rule_type=RuleType.QUERY,
    # replace the message with the corresponding value for this rule, gotten from the excel sheet.
    message="Please check and either amend data or provide a reason: When there is only one assessment on the episode and the factors code “21 No factors identified” has been used for the completed assessment, the reason for closure ‘RC8’ or 'RC9' should be used.",
    # The column names tend to be the words within the < > signs in the github issue description.
    affected_fields=[ReasonForClosure, AssessmentFactors],
)
def validate(
    data_container: Mapping[CINTable, pd.DataFrame], rule_context: RuleContext
):
    # PREPARING DATA

    df_ass = data_container[Assessments].copy()
    df_asslist = data_container[AssessmentFactorsList].copy()
    df_cin = data_container[CINdetails].copy()

    # Before you begin, rename the index so that the initial row positions can be kept intact.
    df_ass.index.name = "ROW_ID"
    df_asslist.index.name = "ROW_ID"
    df_cin.index.name = "ROW_ID"

    # Resetting the index causes the ROW_IDs to become columns of their respective DataFrames
    # so that they can come along when the merge is done.
    df_ass.reset_index(inplace=True)
    df_asslist.reset_index(inplace=True)
    df_cin.reset_index(inplace=True)

    # lOGIC
    # Within a <CINDetails> group, if there is only one <Assessment> group present and <AssessmentFactors> (N00181) = “21”, <ReasonForClosure> (N00103) must should = RC8 or RC9.

    # Eliminates rows with more than 1 assessment per CINdetails group by determining if there's more than 1 AssessmentID per CINdetailsID per child
    df_ass_merged = df_ass.merge(df_ass, on=["LAchildID", "CINdetailsID"])
    df_ass_merged = df_ass_merged[
        (df_ass_merged["AssessmentID_x"] != df_ass_merged["AssessmentID_y"])
    ]
    more_than_1_ass = df_ass_merged["ROW_ID_x"].tolist()

    df_ass = df_ass[~df_ass["ROW_ID"].isin(more_than_1_ass)]

    df_ass_merged = df_ass.merge(
        df_asslist[["LAchildID", "CINdetailsID", "AssessmentID", "AssessmentFactor"]],
        on=["LAchildID", "CINdetailsID", "AssessmentID"],
    )

    df_ass_merged = df_ass_merged[
        (df_ass_merged[AssessmentFactor] == "2B")
        | (df_ass_merged[AssessmentFactor] == "21 No factors identified")
        | (df_ass_merged[AssessmentFactor].str.contains("21"))
    ]

    # left merge means that only the filtered cin children will be considered and there is no possibility of additonal children coming in from other tables.

    # get only the CINdetails groups with AssessmentFactors including 21.
    merged_df = df_ass_merged.merge(
        df_cin,
        on=[LAchildID, "CINdetailsID"],
        how="left",
        suffixes=["_ass", "_cin"],
    )

    # Fails rows where reason for closure is not RC8 or RC9.
    condition = ~merged_df["ReasonForClosure"].isin(["RC8", "RC9"])

    # get all the data that fits the failing condition.
    merged_df = merged_df[condition].reset_index()

    # create an identifier for each error instance.
    merged_df["ERROR_ID"] = tuple(
        zip(
            merged_df[LAchildID],
            merged_df[CINdetailsID],
            merged_df[AssessmentID],
            merged_df[ReasonForClosure],
        )
    )

    # The merges were done on copies of df_ass, and df_cin so that the column names in dataframes themselves aren't affected by the suffixes.
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
        table=Assessments, columns=[AssessmentFactors], row_df=df_ass_issues
    )
    rule_context.push_type_2(
        table=CINdetails, columns=[ReasonForClosure], row_df=df_cin_issues
    )


def test_validate():
    # Create some sample data such that some values pass the validation and some fail.
    sample_ass = pd.DataFrame(
        [
            {
                "LAchildID": "child1",
                "AssessmentFactors": "21",  # 0 pass
                "CINdetailsID": "cinID1",
                "AssessmentID": "11",
                "AssessmentActualStartDate": "5/12/1993",
            },
            {
                "LAchildID": "child1",
                "AssessmentFactors": "BOO",  # 1 ignore: factor!=21
                "CINdetailsID": "cinID2",
                "AssessmentID": "12",
                "AssessmentActualStartDate": "5/12/1993",
            },
            {
                "LAchildID": "child2",
                "AssessmentFactors": "BOO",  # 2 ignore: factor!=21
                "CINdetailsID": "cinID1",
                "AssessmentID": "21",
                "AssessmentActualStartDate": "5/12/1993",
            },
            {
                "LAchildID": "child3",
                "AssessmentFactors": "21",  # 3 fail. reason!=RC8
                "CINdetailsID": "cinID1",
                "AssessmentID": "31",
                "AssessmentActualStartDate": "5/12/1993",
            },
            {  # absent
                "LAchildID": "child3",
                "AssessmentFactors": pd.NA,  # 4 ignore: factor!=21
                "CINdetailsID": "cinID2",
                "AssessmentID": "32",
                "AssessmentActualStartDate": "5/12/1993",
            },
            {  # fail
                "LAchildID": "child3",
                "AssessmentFactors": "21",  # 5 fail. reason!=RC8
                "CINdetailsID": "cinID3",
                "AssessmentID": "33",
                "AssessmentActualStartDate": "5/12/1993",
            },
            {
                "LAchildID": "child3",
                "AssessmentFactors": "21",  # ignore: more than one assessment in CIN episode
                "CINdetailsID": "cinID4",
                "AssessmentID": "34",
                "AssessmentActualStartDate": "5/12/1993",
            },
            {
                "LAchildID": "child3",
                "AssessmentFactors": "20",  # 6 ignore: factor!=21
                "CINdetailsID": "cinID4",
                "AssessmentID": "35",
                "AssessmentActualStartDate": "5/12/1994",
            },
        ]
    )
    sample_cin = pd.DataFrame(
        [
            {  # 0 pass
                "LAchildID": "child1",
                "ReasonForClosure": "RC8",
                "CINdetailsID": "cinID1",
            },
            {  # 1 ignored
                "LAchildID": "child1",
                "ReasonForClosure": "RC3",
                "CINdetailsID": "cinID2",
            },
            {  # 2 pass
                "LAchildID": "child2",
                "ReasonForClosure": "RC8",
                "CINdetailsID": "cinID1",
            },
            {  # 3 fail
                "LAchildID": "child3",
                "ReasonForClosure": "RC10",
                "CINdetailsID": "cinID1",
            },
            {  # 4, ignored
                "LAchildID": "child3",
                "ReasonForClosure": "RC9",
                "CINdetailsID": "cinID2",
            },
            {  # 5 fail
                "LAchildID": "child3",
                "ReasonForClosure": "RC10",
                "CINdetailsID": "cinID3",
            },
            {  # 6 pass
                "LAchildID": "child3",
                "ReasonForClosure": "RC10",
                "CINdetailsID": "cinID4",
            },
        ]
    )

    # if rule requires columns containing date values, convert those columns to datetime objects first. Do it here in the test_validate function, not above.

    # Run the rule function, passing in our sample data.
    result = run_rule(
        validate,
        {
            Assessments: sample_ass,
            AssessmentFactorsList: sample_ass.rename(
                columns={"AssessmentFactors": "AssessmentFactor"}
            ),
            CINdetails: sample_cin,
        },
    )

    # Use .type2_issues to check for the result of .push_type2_issues() which you used above.
    issues_list = result.type2_issues
    assert len(issues_list) == 2
    # the function returns a list on NamedTuples where each NamedTuple contains (table, column_list, df_issues)
    # pick any table and check it's values. the tuple in location 1 will contain the Section47 columns because that's the second thing pushed above.
    issues = issues_list[1]

    # get table name and check it. Replace Section47 with the name of your table.
    issue_table = issues.table
    assert issue_table == CINdetails

    # check that the right columns were returned. Replace DateOfInitialCPC  with a list of your columns.
    issue_columns = issues.columns
    assert issue_columns == [ReasonForClosure]

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
                    "31",  # AssessmentID
                    "RC10",
                ),
                "ROW_ID": [3],
            },
            {
                "ERROR_ID": (
                    "child3",
                    "cinID3",
                    "33",
                    "RC10",
                ),
                "ROW_ID": [5],
            },
        ]
    )
    assert issue_rows.equals(expected_df)

    # Check that the rule definition is what you wrote in the context above.

    # replace 8873Q with the rule code and put the appropriate message in its place too.
    assert result.definition.code == "8873Q"
    assert (
        result.definition.message
        == "Please check and either amend data or provide a reason: When there is only one assessment on the episode and the factors code “21 No factors identified” has been used for the completed assessment, the reason for closure ‘RC8’ or 'RC9' should be used."
    )
