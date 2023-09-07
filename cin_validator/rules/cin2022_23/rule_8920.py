from typing import Mapping

import pandas as pd

from cin_validator.rule_engine import CINTable, RuleContext, rule_definition
from cin_validator.test_engine import run_rule

# Get tables and columns of interest from the CINTable object defined in rule_engine/__api.py

ChildProtectionPlans = CINTable.ChildProtectionPlans
CPPendDate = ChildProtectionPlans.CPPendDate
LAchildID = ChildProtectionPlans.LAchildID

ChildIdentifiers = CINTable.ChildIdentifiers
PersonDeathDate = ChildIdentifiers.PersonDeathDate
LAchildID = ChildIdentifiers.LAchildID


# define characteristics of rule
@rule_definition(
    code="8920",
    module=CINTable.ChildProtectionPlans,
    message="Child Protection Plan cannot end after the child’s Date of Death",
    affected_fields=[
        PersonDeathDate,
        CPPendDate,
    ],
)
def validate(
    data_container: Mapping[CINTable, pd.DataFrame], rule_context: RuleContext
):
    # PREPARING DATA

    df_cpp = data_container[ChildProtectionPlans].copy()
    df_child = data_container[ChildIdentifiers].copy()

    # Before you begin, rename the index so that the initial row positions can be kept intact.
    df_cpp.index.name = "ROW_ID"
    df_child.index.name = "ROW_ID"

    # Resetting the index causes the ROW_IDs to become columns of their respective DataFrames
    # so that they can come along when the merge is done.
    df_cpp.reset_index(inplace=True)
    df_child.reset_index(inplace=True)

    # lOGIC
    # Implement rule logic as described by the Github issue.
    # Put the description as a comment above the implementation as shown.

    # If <PersonDeathDate> (N00108) is present, then <CPPendDate> (N00115) must be on or before <PersonDeathDate> (N00108)
    # Issues dfs should return rows where PersonDeathDate is less than or equal to the CPPendDate

    #  Create dataframes which only have rows with CP plans, and which should have one plan per row.
    df_cpp = df_cpp[df_cpp[CPPendDate].notna()]
    df_child = df_child[df_child[PersonDeathDate].notna()]

    #  Merge tables to get corresponding CP plan group for the child
    df_merged = df_cpp.merge(
        df_child,
        left_on=["LAchildID"],
        right_on=["LAchildID"],
        how="left",
        suffixes=("_cpp", "_child"),
    )

    #  Get rows where CPPendDate is after PersonDeathDate
    condition = df_merged[CPPendDate] > df_merged[PersonDeathDate]
    df_merged = df_merged[condition].reset_index()

    # create an identifier for each error instance.
    # In this case, the rule is checked for each CPPendDate against the PersonDeathDate for that child.
    # A child may have multiple CP Plans but only 1 should be current at anytime and requires a CPPendDate

    df_merged["ERROR_ID"] = tuple(
        zip(df_merged[LAchildID], df_merged[CPPendDate], df_merged[PersonDeathDate])
    )

    # The merges were done on copies of df_cpp and df_child so that the column names in dataframes themselves aren't affected by the suffixes.
    # we can now map the suffixes columns to their corresponding source tables such that the failing ROW_IDs and ERROR_IDs exist per table.
    df_cpp_issues = (
        df_cpp.merge(df_merged, left_on="ROW_ID", right_on="ROW_ID_cpp")
        .groupby("ERROR_ID", group_keys=False)["ROW_ID"]
        .apply(list)
        .reset_index()
    )
    df_reviews_issues = (
        df_child.merge(df_merged, left_on="ROW_ID", right_on="ROW_ID_child")
        .groupby("ERROR_ID", group_keys=False)["ROW_ID"]
        .apply(list)
        .reset_index()
    )

    # Ensure that you maintain the ROW_ID, and ERROR_ID column names which are shown above. They are keywords in this project.
    rule_context.push_type_2(
        table=ChildProtectionPlans, columns=[CPPendDate], row_df=df_cpp_issues
    )
    rule_context.push_type_2(
        table=ChildIdentifiers, columns=[PersonDeathDate], row_df=df_reviews_issues
    )


def test_validate():
    # Create some sample data such that some values pass the validation and some fail.
    sample_cpp = pd.DataFrame(
        [
            {
                "LAchildID": "child1",
                "CPPendDate": "26/05/2000",  # Fails as PersonDeathDate is before the CPPendDate
            },
            {
                "LAchildID": "child2",
                "CPPendDate": "27/06/2002",  # Fails as PersonDeathDate is before the CPPendDate
            },
            {
                "LAchildID": "child3",
                "CPPendDate": "07/02/2001",  # Fails as PersonDeathDate is before the CPPendDate
            },
            {
                "LAchildID": "child4",
                "CPPendDate": "26/05/2000",  # Passes as PersonDeathDate is after the CPPendDate
            },
            {
                "LAchildID": "child5",
                "CPPendDate": "26/05/2000",  # Passes as PersonDeathDate is after the CPPendDate
            },
            {
                "LAchildID": "child6",
                "CPPendDate": pd.NA,  # Ignored as rows with no CPPendDate are dropped (this is picked up by other rules)
            },
            {
                "LAchildID": "child7",
                "CPPendDate": "14/03/2001",  # Ignored as rows with no PersonDeathDate are dropped (this is picked up by other rules)
            },
        ]
    )
    sample_children = pd.DataFrame(
        [
            {
                "LAchildID": "child1",
                "PersonDeathDate": "25/05/2000",  # Fails as PersonDeathDate is before the CPPendDate
            },
            {
                "LAchildID": "child2",
                "PersonDeathDate": "29/05/2000",  # Fails as PersonDeathDate is before the CPPendDate
            },
            {
                "LAchildID": "child3",
                "PersonDeathDate": "26/03/2000",  # Fails as PersonDeathDate is before the CPPendDate
            },
            {
                "LAchildID": "child4",
                "PersonDeathDate": "30/05/2000",  # Passes as PersonDeathDate is after the CPPendDate
            },
            {
                "LAchildID": "child5",
                "PersonDeathDate": "27/05/2000",  # Passes as PersonDeathDate is after the CPPendDate
            },
            {
                "LAchildID": "child6",
                "PersonDeathDate": "26/05/2000",  # Ignored as rows with no CPPendDate are dropped (this is picked up by other rules)
            },
            {
                "LAchildID": "child7",
                "PersonDeathDate": pd.NA,  # Ignored as rows with no PersonDeathDate are dropped (this is picked up by other rules)
            },
        ]
    )

    # If rule requires columns containing date values, convert those columns to datetime objects first. Do it here in the test_validate function, not above.
    sample_cpp[CPPendDate] = pd.to_datetime(
        sample_cpp[CPPendDate], format="%d/%m/%Y", errors="coerce"
    )
    sample_children["PersonDeathDate"] = pd.to_datetime(
        sample_children["PersonDeathDate"], format="%d/%m/%Y", errors="coerce"
    )

    # Run the rule function, passing in our sample data.
    result = run_rule(
        validate,
        {
            ChildProtectionPlans: sample_cpp,
            ChildIdentifiers: sample_children,
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
    assert issue_table == ChildIdentifiers

    # check that the right columns were returned. Replace CPPreviewDate  with a list of your columns.
    issue_columns = issues.columns
    assert issue_columns == [PersonDeathDate]

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
                    "child1",  # ChildID
                    # CPP End Date
                    pd.to_datetime("26/05/2000", format="%d/%m/%Y", errors="coerce"),
                    # Person Death Date
                    pd.to_datetime("25/05/2000", format="%d/%m/%Y", errors="coerce"),
                ),
                "ROW_ID": [0],
            },
            {
                "ERROR_ID": (
                    "child2",  # ChildID
                    # CPP End Date
                    pd.to_datetime("27/06/2002", format="%d/%m/%Y", errors="coerce"),
                    # Person Death Date
                    pd.to_datetime("29/05/2000", format="%d/%m/%Y", errors="coerce"),
                ),
                "ROW_ID": [1],
            },
            {
                "ERROR_ID": (
                    "child3",  # ChildID
                    # CPP End Date
                    pd.to_datetime("07/02/2001", format="%d/%m/%Y", errors="coerce"),
                    # Person Death Date
                    pd.to_datetime("26/03/2000", format="%d/%m/%Y", errors="coerce"),
                ),
                "ROW_ID": [2],
            },
        ]
    )
    assert issue_rows.equals(expected_df)

    # Check that the rule definition is what you wrote in the context above.

    # replace 2885 with the rule code and put the appropriate message in its place too.
    assert result.definition.code == "8920"
    assert (
        result.definition.message
        == "Child Protection Plan cannot end after the child’s Date of Death"
    )
