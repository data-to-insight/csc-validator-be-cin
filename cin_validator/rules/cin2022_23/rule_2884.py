from typing import Mapping

import pandas as pd

from cin_validator.rule_engine import CINTable, RuleContext, rule_definition
from cin_validator.test_engine import run_rule

# Get tables and columns of interest from the CINTable object defined in rule_engine/__api.py

CINdetails = CINTable.CINdetails
Section47 = CINTable.Section47

LAchildID = CINdetails.LAchildID
CINdetailsID = CINdetails.CINdetailsID
DateOfInitialCPC = Section47.DateOfInitialCPC
DateOfInitialCPC = CINdetails.DateOfInitialCPC


# define characteristics of rule
@rule_definition(
    # write the rule code here, in place of 2884
    code=2884,
    # replace Section47 with the value in the module column of the excel sheet corresponding to this rule .
    # Note that even if multiple tables are involved, one table will be named in the module column.
    module=CINTable.Section47,
    # replace the message with the corresponding value for this rule, gotten from the excel sheet.
    message="An initial child protection conference is recorded at both the S47 and CIN Details level and it should only be recorded in one",
    # The column names tend to be the words within the < > signs in the github issue description.
    affected_fields=[
        DateOfInitialCPC,
    ],
)
def validate(
    data_container: Mapping[CINTable, pd.DataFrame], rule_context: RuleContext
):

    df_47 = data_container[Section47].copy()
    df_cin = data_container[CINdetails].copy()

    df_47.index.name = "ROW_ID"
    df_cin.index.name = "ROW_ID"

    df_47.reset_index(inplace=True)
    df_cin.reset_index(inplace=True)

    merged_df = df_cin.merge(
        df_47,
        on=[LAchildID],
        suffixes=["_cin", "_47"],
        # the suffixes apply to all the columns not "merged on". That is, DateOfInitialCPC
    )

    condition = merged_df["DateOfInitialCPC_cin"] == merged_df["DateOfInitialCPC_47"]
    merged_df = merged_df[condition].reset_index()

    merged_df["ERROR_ID"] = tuple(
        zip(
            merged_df[LAchildID],
            merged_df["CINdetailsID_47"],
            merged_df["DateOfInitialCPC_cin"],
        )
    )

    # we can now map the suffixes columns to their corresponding source tables such that the failing ROW_IDs and ERROR_IDs exist per table.
    df_47_issues = (
        df_47.merge(merged_df, left_on="ROW_ID", right_on="ROW_ID_47")
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
        table=Section47, columns=[DateOfInitialCPC], row_df=df_47_issues
    )
    rule_context.push_type_2(
        table=CINdetails, columns=[DateOfInitialCPC], row_df=df_cin_issues
    )


def test_validate():
    # Create some sample data such that some values pass the validation and some fail.
    sample_section47 = pd.DataFrame(
        [
            {  # 0 fail: datecpc == (child1, cinID2)'s datecpc in cindetails table
                "LAchildID": "child1",
                "DateOfInitialCPC": "26/05/2000",
                "CINdetailsID": "cinID1",
            },
            {  # 1 fail: datecpc == (child1, cinID2)'s datecpc in cindetails table
                "LAchildID": "child1",
                "DateOfInitialCPC": "26/05/2000",
                "CINdetailsID": "cinID2",
            },
            {  # 2
                "LAchildID": "child2",
                "DateOfInitialCPC": "30/05/2000",
                "CINdetailsID": "cinID1",
            },
            {  # 3
                "LAchildID": "child3",
                "DateOfInitialCPC": "27/05/2000",
                "CINdetailsID": "cinID1",
            },
            {  # 4 fail: datecpc == (child3, cinID2)'s datecpc in cindetails table
                "LAchildID": "child3",
                "DateOfInitialCPC": "26/05/2000",
                "CINdetailsID": "cinID2",
            },
            {  # 5 fail: datecpc == (child3, cinID2)'s datecpc in cindetails table
                "LAchildID": "child3",
                "DateOfInitialCPC": "26/05/2000",
                "CINdetailsID": "cinID3",
            },
            {  # 6
                "LAchildID": "child3",
                "DateOfInitialCPC": pd.NA,
                "CINdetailsID": "cinID4",
            },
        ]
    )
    sample_cin_details = pd.DataFrame(
        [
            {  # 0
                "LAchildID": "child1",
                "DateOfInitialCPC": "26/10/1999",
                "CINdetailsID": "cinID1",
            },
            {  # 1 fail
                "LAchildID": "child1",
                "DateOfInitialCPC": "26/05/2000",
                "CINdetailsID": "cinID2",
            },
            {  # 2
                "LAchildID": "child2",
                "DateOfInitialCPC": "26/05/2000",
                "CINdetailsID": "cinID1",
            },
            {  # 3
                "LAchildID": "child3",
                "DateOfInitialCPC": "28/05/2000",
                "CINdetailsID": "cinID1",
            },
            {  # 4 fail
                "LAchildID": "child3",
                "DateOfInitialCPC": "26/05/2000",
                "CINdetailsID": "cinID2",
            },
            {  # 5
                "LAchildID": "child3",
                "DateOfInitialCPC": "26/05/2003",
                "CINdetailsID": "cinID3",
            },
            {  # 6
                "LAchildID": "child3",
                "DateOfInitialCPC": "14/03/2001",
                "CINdetailsID": "cinID4",
            },
        ]
    )
    # if rule requires columns containing date values, convert those columns to datetime objects first. Do it here in the test_validate function, not above.
    sample_section47["DateOfInitialCPC"] = pd.to_datetime(
        sample_section47["DateOfInitialCPC"], format="%d/%m/%Y", errors="coerce"
    )
    sample_cin_details["DateOfInitialCPC"] = pd.to_datetime(
        sample_cin_details["DateOfInitialCPC"], format="%d/%m/%Y", errors="coerce"
    )

    # Run the rule function, passing in our sample data.
    result = run_rule(
        validate,
        {
            Section47: sample_section47,
            CINdetails: sample_cin_details,
        },
    )

    # Use .type2_issues to check for the result of .push_type2_issues() which you used above.
    issues_list = result.type2_issues
    assert len(issues_list) == 2
    # the function returns a list on NamedTuples where each NamedTuple contains (table, column_list, df_issues)
    # pick any table and check it's values. the tuple in location 1 will contain the Section47 columns because that's the second thing pushed above.
    issues = issues_list[0]

    # get table name and check it. Replace Section47 with the name of your table.
    issue_table = issues.table
    assert issue_table == Section47

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
                    "child1",
                    "cinID1",
                    pd.to_datetime("26/05/2000", format="%d/%m/%Y", errors="coerce"),
                ),
                "ROW_ID": [0],
            },
            {
                "ERROR_ID": (
                    "child1",
                    "cinID2",
                    pd.to_datetime("26/05/2000", format="%d/%m/%Y", errors="coerce"),
                ),
                "ROW_ID": [1],
            },
            {
                "ERROR_ID": (
                    "child3",
                    "cinID2",
                    pd.to_datetime("26/05/2000", format="%d/%m/%Y", errors="coerce"),
                ),
                "ROW_ID": [4],
            },
            {
                "ERROR_ID": (
                    "child3",
                    "cinID3",
                    pd.to_datetime("26/05/2000", format="%d/%m/%Y", errors="coerce"),
                ),
                "ROW_ID": [5],
            },
        ]
    )
    assert issue_rows.equals(expected_df)

    # Check that the rule definition is what you wrote in the context above.

    # replace 2884 with the rule code and put the appropriate message in its place too.
    assert result.definition.code == 2884
    assert (
        result.definition.message
        == "An initial child protection conference is recorded at both the S47 and CIN Details level and it should only be recorded in one"
    )
