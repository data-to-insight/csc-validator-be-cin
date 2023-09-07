from typing import Mapping

import pandas as pd

from cin_validator.rule_engine import CINTable, RuleContext, rule_definition
from cin_validator.test_engine import run_rule

# Get tables and columns of interest from the CINTable object defined in rule_engine/__api.py

CINdetails = CINTable.CINdetails
Section47 = CINTable.Section47
Assessments = CINTable.Assessments

AssessmentAuthorisationDate = Assessments.AssessmentAuthorisationDate
AssessmentActualStartDate = Assessments.AssessmentActualStartDate
S47ActualStartDate = Section47.S47ActualStartDate
ReferralNFA = CINdetails.ReferralNFA
DateOfInitialCPC = CINdetails.DateOfInitialCPC

LAchildID = CINdetails.LAchildID
CINdetailsID = CINdetails.CINdetailsID


# define characteristics of rule
@rule_definition(
    # write the rule code here, in place of '8831'
    code="8831",
    # replace CINdetails with the value in the module column of the excel sheet corresponding to this rule .
    # Note that even if multiple tables are involved, one table will be named in the module column.
    module=CINTable.CINdetails,
    # replace the message with the corresponding value for this rule, gotten from the excel sheet.
    message="Activity is recorded against a case marked as a referral with no further action",
    # The column names tend to be the words within the < > signs in the github issue description.
    affected_fields=[
        AssessmentActualStartDate,
        AssessmentAuthorisationDate,
        S47ActualStartDate,
        DateOfInitialCPC,
    ],
)
def validate(
    data_container: Mapping[CINTable, pd.DataFrame], rule_context: RuleContext
):
    # PREPARING DATA

    df_ass = data_container[Assessments].copy()
    df_47 = data_container[Section47].copy()
    df_cin = data_container[CINdetails].copy()

    # Before you begin, rename the index so that the initial row positions can be kept intact.
    df_47.index.name = "ROW_ID"
    df_cin.index.name = "ROW_ID"
    df_ass.index.name = "ROW_ID"

    # Resetting the index causes the ROW_IDs to become columns of their respective DataFrames
    # so that they can come along when the merge is done.
    df_47.reset_index(inplace=True)
    df_cin.reset_index(inplace=True)
    df_ass.reset_index(inplace=True)

    # If a <CINdetails> module has <ReferralNFA> (N00112) = true or 1, then it cannot have any of the following:
    # <AssessmentActualStartDate> (N00159)
    # <AssessmentAuthorisationDate> (N00160)
    # <S47ActualStartDate> (N00148)
    # <DateOfInitialCPC> (N00110)

    df_cin = df_cin[df_cin[ReferralNFA].isin(["true", "1"])]

    # Check columns in Section47 table
    df_cin_47 = df_cin.merge(
        df_47,
        on=[
            "LAchildID",
            "CINdetailsID",
        ],
        how="left",
        suffixes=["_cin", "_47"],
    )

    # filter out rows that have an S47ActualStartDate or DateOfInitialCPC from the CINdetails module
    condition_1 = (
        df_cin_47["DateOfInitialCPC_cin"].notna()
        | df_cin_47[S47ActualStartDate].notna()
    )
    df_cin_47 = df_cin_47[condition_1]

    df_cin_47["ERROR_ID"] = tuple(zip(df_cin_47[LAchildID], df_cin_47[CINdetailsID]))

    df_47_issues = (
        df_47.merge(df_cin_47, left_on="ROW_ID", right_on="ROW_ID_47")
        .groupby("ERROR_ID", group_keys=False)["ROW_ID"]
        .apply(list)
        .reset_index()
    )
    cin_issues_47 = (
        df_cin.merge(df_cin_47, left_on="ROW_ID", right_on="ROW_ID_cin")
        .groupby("ERROR_ID", group_keys=False)["ROW_ID"]
        .apply(list)
        .reset_index()
    )

    # Check columns in Assessments table
    df_cin_ass = df_cin.merge(
        df_ass,
        on=["LAchildID", "CINdetailsID"],
        how="left",
        suffixes=["_cin", "_ass"],
    )
    # filter out rows that have an AssessmentActualStartDate or AssessmentAuthorisationDate
    condition_2 = df_cin_ass[AssessmentActualStartDate].notna()
    condition_3 = df_cin_ass[AssessmentAuthorisationDate].notna()
    df_cin_ass = df_cin_ass[condition_2 | condition_3]
    df_cin_ass["ERROR_ID"] = tuple(zip(df_cin_ass[LAchildID], df_cin_ass[CINdetailsID]))
    df_ass_issues = (
        df_ass.merge(df_cin_ass, left_on="ROW_ID", right_on="ROW_ID_ass")
        .groupby("ERROR_ID", group_keys=False)["ROW_ID"]
        .apply(list)
        .reset_index()
    )
    cin_issues_ass = (
        df_cin.merge(df_cin_ass, left_on="ROW_ID", right_on="ROW_ID_cin")
        .groupby("ERROR_ID", group_keys=False)["ROW_ID"]
        .apply(list)
        .reset_index()
    )

    df_cin_issues = pd.concat([cin_issues_47, cin_issues_ass])
    # in case a value was flagged in both table combinations, it'll exist twice so deduplicate df_cin_issues
    df_cin_issues.drop_duplicates("ERROR_ID")
    df_cin_issues.reset_index(inplace=True, drop=True)

    # Ensure that you maintain the ROW_ID, and ERROR_ID column names which are shown above. They are keywords in this project.
    rule_context.push_type_2(
        table=CINdetails, columns=[DateOfInitialCPC], row_df=df_cin_issues
    )
    rule_context.push_type_2(
        table=Assessments, columns=[LAchildID], row_df=df_ass_issues
    )
    rule_context.push_type_2(table=Section47, columns=[LAchildID], row_df=df_47_issues)


def test_validate():
    # Create some sample data such that some values pass the validation and some fail.
    sample_section47 = pd.DataFrame(
        [
            {
                "LAchildID": "child1",
                "DateOfInitialCPC": pd.NA,
                "CINdetailsID": "cinID2",
            },
            {
                "LAchildID": "child2",  # fails for having a module
                "CINdetailsID": "cinID2",
                "DateOfInitialCPC": pd.NA,
                "S47ActualStartDate": "01/01/2000",
            },
            {
                "LAchildID": "child2",
                "DateOfInitialCPC": pd.NA,
                "CINdetailsID": "cinID1",
            },
            {
                "LAchildID": "child3",
                "DateOfInitialCPC": pd.NA,
                "CINdetailsID": "cinID1",
            },
            {
                "LAchildID": "child3",
                "DateOfInitialCPC": pd.NA,
                "CINdetailsID": "cinID2",
            },
            {
                "LAchildID": "child3",
                "DateOfInitialCPC": pd.NA,
                "CINdetailsID": "cinID3",
            },
            {
                "LAchildID": "child3",
                "DateOfInitialCPC": pd.NA,
                "CINdetailsID": "cinID4",
            },
        ]
    )
    sample_cin_details = pd.DataFrame(
        [
            {
                "LAchildID": "child1",  # 0 fail: has AssessmentAuthorisationDate
                "DateOfInitialCPC": pd.NA,
                "CINdetailsID": "cinID1",
                "ReferralNFA": "1",
            },
            {
                "LAchildID": "child2",  # 1 fail: has S47ActualStartDate
                "DateOfInitialCPC": pd.NA,
                "CINdetailsID": "cinID2",
                "ReferralNFA": "true",
            },
            {
                "LAchildID": "child3",  # 2 fail: has AssessmentActualStartDate
                "DateOfInitialCPC": pd.NA,
                "CINdetailsID": "cinID3",
                "ReferralNFA": "1",
            },
            {
                "LAchildID": "child4",
                "DateOfInitialCPC": "28/05/2000",  # 3 fails for having initial cpc
                "CINdetailsID": "cinID4",
                "ReferralNFA": "true",
            },
            {
                "LAchildID": "child3",
                "DateOfInitialCPC": "26/05/2000",
                "CINdetailsID": "cinID2",
                "ReferralNFA": "false",  # 4 ignore
            },
            {
                "LAchildID": "child3",
                "DateOfInitialCPC": "26/05/2003",
                "CINdetailsID": "cinID8",
                "ReferralNFA": "false",  # 5 ignore
            },
            {  # 6 pass
                "LAchildID": "child3",
                "DateOfInitialCPC": "14/03/2001",
                "CINdetailsID": "cinID4",
                "ReferralNFA": "false",  # 6 ignore
            },
        ]
    )
    sample_ass = pd.DataFrame(
        [
            {
                "LAchildID": "child1",  # fails for having a module
                "CINdetailsID": "cinID1",
                "AssessmentAuthorisationDate": "01/01/2000",
                "AssessmentActualStartDate": pd.NA,
            },
            {
                "LAchildID": "child3",  # fails for having a module
                "CINdetailsID": "cinID3",
                "AssessmentAuthorisationDate": "01/01/2000",
                "AssessmentActualStartDate": "01/01/2000",
            },
            {
                "LAchildID": "child1",
                "CINdetailsID": "cinID2",
                "AssessmentAuthorisationDate": pd.NA,
                "AssessmentActualStartDate": pd.NA,
            },
            {
                "LAchildID": "child1",
                "CINdetailsID": "cinID3",
                "AssessmentAuthorisationDate": pd.NA,
                "AssessmentActualStartDate": pd.NA,
            },
            {
                "LAchildID": "child1",
                "CINdetailsID": "cinID4",
                "AssessmentAuthorisationDate": pd.NA,
                "AssessmentActualStartDate": pd.NA,
            },
        ]
    )
    # if rule requires columns containing date values, convert those columns to datetime objects first. Do it here in the test_validate function, not above.

    sample_cin_details["DateOfInitialCPC"] = pd.to_datetime(
        sample_cin_details["DateOfInitialCPC"], format="%d/%m/%Y", errors="coerce"
    )
    sample_section47["DateOfInitialCPC"] = pd.to_datetime(
        sample_section47["DateOfInitialCPC"], format="%d/%m/%Y", errors="coerce"
    )
    sample_section47["S47ActualStartDate"] = pd.to_datetime(
        sample_section47["S47ActualStartDate"], format="%d/%m/%Y", errors="coerce"
    )
    sample_ass["AssessmentAuthorisationDate"] = pd.to_datetime(
        sample_ass["AssessmentAuthorisationDate"], format="%d/%m/%Y", errors="coerce"
    )
    sample_ass["AssessmentActualStartDate"] = pd.to_datetime(
        sample_ass["AssessmentActualStartDate"], format="%d/%m/%Y", errors="coerce"
    )

    # Run the rule function, passing in our sample data.
    result = run_rule(
        validate,
        {
            Section47: sample_section47,
            CINdetails: sample_cin_details,
            Assessments: sample_ass,
        },
    )

    # Use .type2_issues to check for the result of .push_type2_issues() which you used above.
    issues_list = result.type2_issues
    assert len(issues_list) == 3
    # the function returns a list on NamedTuples where each NamedTuple contains (table, column_list, df_issues)
    # pick any table and check it's values. the tuple in location 0 will contain the CINdetails columns because that's the first thing pushed above.
    issues = issues_list[0]

    # get table name and check it. Replace Section47 with the name of your table.
    issue_table = issues.table
    assert issue_table == CINdetails

    # check that the right columns were returned. Replace DateOfInitialCPC  with a list of your columns.
    issue_columns = issues.columns
    assert issue_columns == [DateOfInitialCPC]

    # check that the location linking dataframe was formed properly.
    issue_rows = issues.row_df
    # replace 4 with the number of failing points you expect from the sample data.
    assert len(issue_rows) == 4
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
                    "cinID2",  # CINdetailsID
                ),
                "ROW_ID": [1],
            },
            {
                "ERROR_ID": (
                    "child4",  # ChildID
                    "cinID4",  # CINdetailsID
                ),
                "ROW_ID": [3],
            },
            {
                "ERROR_ID": (
                    "child1",  # ChildID
                    "cinID1",  # CINdetailsID
                ),
                "ROW_ID": [0],
            },
            {
                "ERROR_ID": (
                    "child3",  # ChildID
                    "cinID3",  # CINdetailsID
                ),
                "ROW_ID": [2],
            },
        ]
    )
    assert issue_rows.equals(expected_df)

    # Check that the rule definition is what you wrote in the context above.

    # replace '8831' with the rule code and put the appropriate message in its place too.
    assert result.definition.code == "8831"
    assert (
        result.definition.message
        == "Activity is recorded against a case marked as a referral with no further action"
    )
