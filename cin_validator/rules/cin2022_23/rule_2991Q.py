from typing import Mapping

import pandas as pd

from cin_validator.rule_engine import CINTable, RuleContext, rule_definition, RuleType
from cin_validator.test_engine import run_rule
from cin_validator.utils import make_census_period

# Get tables and columns of interest from the CINTable object defined in rule_engine/__api.py

Assessments = CINTable.CINdetails
Section47 = CINTable.Section47


LAchildID = Assessments.LAchildID
CINdetailsID = Assessments.CINdetailsID

# define characteristics of rule
@rule_definition(
    code="2991Q",
    module=CINTable.CINdetails,
    rule_type=RuleType.QUERY,
    message="Please check: A Section 47 module is recorded and there is no assessment on the episode",
    affected_fields=[
        CINdetailsID,
    ],
)
def validate(
    data_container: Mapping[CINTable, pd.DataFrame], rule_context: RuleContext
):
    df_ass = data_container[Assessments].copy()
    df_47 = data_container[Section47].copy()

    df_ass.index.name = "ROW_ID"
    df_47.index.name = "ROW_ID"

    df_ass.reset_index(inplace=True)
    df_47.reset_index(inplace=True)

    # If <Section47> module is present then <Assessment> module should be present

    # df_ass["has_id"] =  df_ass.where(df_ass['CINdetailsID'].notna(), "yes", "no")
    df_ass["has_id"] = tuple(
        zip(
            df_ass[LAchildID],
            df_ass[CINdetailsID].fillna("no"),
        )
    )

    ids = df_ass["has_id"].to_list()
    # df_472 = df_47.copy()
    df_47["modules"] = tuple(
        zip(
            df_47[LAchildID],
            df_47[CINdetailsID],
        )
    )
    # df_472 = df_472[~df_472["modules"].isin(ids)]
    # print(df_472)
    merged_df = df_47.merge(
        df_ass,
        on=[
            "LAchildID",
        ],
        suffixes=["_47", "_ass"],
        how="left",
    )
    # print(merged_df[["LAchildID", "CINdetailsID_47", "CINdetailsID_ass", ]])

    # TODO try using a tuple
    # Returns rows where there is a section 47 module without an assessments module with a mathcing CINdetailsID
    merged_df = merged_df[~merged_df["modules"].isin(ids)]
    condition = merged_df["CINdetailsID_ass"].isna()

    merged_df = merged_df[condition].reset_index()

    merged_df["ERROR_ID"] = tuple(
        zip(
            merged_df[LAchildID],
            merged_df["CINdetailsID_47"],
        )
    )
    print(merged_df[["LAchildID", "CINdetailsID_47", "CINdetailsID_ass", "ERROR_ID"]])
    # The merges were done on copies of df_cpp, df_47 and df_cin so that the column names in dataframes themselves aren't affected by the suffixes.
    # we can now map the suffixes columns to their corresponding source tables such that the failing ROW_IDs and ERROR_IDs exist per table.
    df_ass_issues = (
        df_ass.merge(merged_df, left_on="ROW_ID", right_on="ROW_ID_47")
        .groupby("ERROR_ID", group_keys=False)["ROW_ID"]
        .apply(list)
        .reset_index()
    )

    df_47_issues = (
        df_47.merge(merged_df, left_on="ROW_ID", right_on="ROW_ID_47")
        .groupby("ERROR_ID", group_keys=False)["ROW_ID"]
        .apply(list)
        .reset_index()
    )

    # Ensure that you maintain the ROW_ID, and ERROR_ID column names which are shown above. They are keywords in this project.
    rule_context.push_type_2(
        table=Assessments, columns=[CINdetailsID], row_df=df_ass_issues
    )
    rule_context.push_type_2(
        table=Section47, columns=[CINdetailsID], row_df=df_47_issues
    )


def test_validate():
    # Create some sample data such that some values pass the validation and some fail.
    sample_ass = pd.DataFrame(
        [
            {
                "LAchildID": "child1",
                "CPPstartDate": "26/05/2000",  # fail
                "CINdetailsID": pd.NA,
            },
            {
                "LAchildID": "child1",
                "CPPstartDate": "27/06/2002",
                "CINdetailsID": "cinID2",
            },
            {
                "LAchildID": "child3",
                "CPPstartDate": "26/05/2000",
                "CINdetailsID": "cinID1",
            },
            {
                "LAchildID": "child3",
                "CPPstartDate": pd.NA,
                "CINdetailsID": "cinID2",
            },
            {
                "LAchildID": "child3",
                "CPPstartDate": "07/02/2001",
                "CINdetailsID": "cinID3",
            },
            {
                "LAchildID": "child3",
                "CPPstartDate": "14/03/2001",
                "CINdetailsID": "cinID4",
            },
            {
                "LAchildID": "child4",
                "CPPstartDate": "14/03/2001",
                "CINdetailsID": "cinID4",
            },
        ]
    )
    sample_section47 = pd.DataFrame(
        [
            {
                "LAchildID": "child1",
                "DateOfInitialCPC": "26/05/2000",
                "CINdetailsID": "cinID1",
            },
            {
                "LAchildID": "child1",
                "DateOfInitialCPC": "26/05/2000",
                "CINdetailsID": "cinID2",
            },
            {
                "LAchildID": "child2",
                "DateOfInitialCPC": "30/05/2000",
                "CINdetailsID": "cinID1",
            },
            {
                "LAchildID": "child3",
                "DateOfInitialCPC": "27/05/2000",
                "CINdetailsID": "cinID1",
            },
            {
                "LAchildID": "child3",
                "DateOfInitialCPC": "26/05/2000",
                "CINdetailsID": "cinID2",
            },
            {
                "LAchildID": "child3",
                "DateOfInitialCPC": "26/05/2000",
                "CINdetailsID": "cinID3",
            },
            {
                "LAchildID": "child3",
                "DateOfInitialCPC": pd.NA,
                "CINdetailsID": pd.NA,
            },
        ]
    )

    # Run the rule function, passing in our sample data.
    result = run_rule(
        validate,
        {
            Assessments: sample_ass,
            Section47: sample_section47,
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
    assert issue_table == Section47

    # check that the right columns were returned. Replace DateOfInitialCPC  with a list of your columns.
    issue_columns = issues.columns
    assert issue_columns == [CINdetailsID]

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
                    "child1",
                    "cinID1",
                ),
                "ROW_ID": [0],
            },
            {
                "ERROR_ID": (
                    "child2",
                    "cinID1",
                ),
                "ROW_ID": [2],
            },
        ]
    )
    assert issue_rows.equals(expected_df)

    # Check that the rule definition is what you wrote in the context above.

    # replace 2885 with the rule code and put the appropriate message in its place too.
    assert result.definition.code == "2991Q"
    assert (
        result.definition.message
        == "Please check: A Section 47 module is recorded and there is no assessment on the episode"
    )
