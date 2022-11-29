from typing import Mapping

import pandas as pd

from cin_validator.rule_engine import CINTable, RuleContext, rule_definition
from cin_validator.test_engine import run_rule
from cin_validator.utils import make_census_period

# Get tables and columns of interest from the CINTable object defined in rule_engine/__api.py

ChildIdentifiers = CINTable.ChildIdentifiers
PersonDeathDate = ChildIdentifiers.PersonDeathDate
LAchildID = ChildIdentifiers.LAchildID

CINplanDates = CINTable.CINplanDates
LAChildId = CINplanDates.LAchildID
CINPlanEndDate = CINplanDates.CINPlanEndDate

# define characteristics of rule
@rule_definition(
    # write the rule code here
    code=4009,
    # replace ChildProtectionPlans with the value in the module column of the excel sheet corresponding to this rule .
    # Note that even if multiple tables are involved, one table will be named in the module column.
    module=CINTable.ChildIdentifiers,
    # replace the message with the corresponding value for this rule, gotten from the excel sheet.
    message="CIN Plan shown as ending after the child's Date of Death.",
    # The column names tend to be the words within the < > signs in the github issue description.
    affected_fields=[
        PersonDeathDate,
        CINPlanEndDate,
    ],
)
def validate(
    data_container: Mapping[CINTable, pd.DataFrame], rule_context: RuleContext
):
    # PREPARING DATA

    # Replace ChildProtectionPlans with the name of the table you need.
    df_ci = data_container[ChildIdentifiers].copy()
    df_cinplan = data_container[CINplanDates].copy()

    # Before you begin, rename the index so that the initial row positions can be kept intact.
    df_ci.index.name = "ROW_ID"
    df_cinplan.index.name = "ROW_ID"

    # Resetting the index causes the ROW_IDs to become columns of their respective DataFrames
    # so that they can come along when the merge is done.
    df_ci.reset_index(inplace=True)
    df_cinplan.reset_index(inplace=True)

    # lOGIC
    # Implement rule logic as described by the Github issue.
    # Put the description as a comment above the implementation as shown.

    # If <PersonDeathDate> (N00108) is present, then  <CINPlanEndDate> (N00690) should be on or before <PersonDeathDate> (N00108)
    # Issues dfs should return rows where PersonDeathDate is less than the CINPlanEndDate

    #  Create dataframes which only have rows with PersonDeathDate and CINPlanEndDate not blank.
    df_ci = df_ci[df_ci[PersonDeathDate].notna()]
    df_cinplan = df_cinplan[df_cinplan[CINPlanEndDate].notna()]

    #  Merge tables to get corresponding CP plan group and reviews
    df_merged = df_ci.merge(
        df_cinplan,
        left_on=["LAchildID"],
        right_on=["LAchildID"],
        how="left",
        suffixes=("_ci", "_cinplan"),
    )

    #  Get rows where PersonDeathDate is less than than CINPlan End Date
    condition = df_merged[PersonDeathDate] < df_merged[CINPlanEndDate]
    df_merged = df_merged[condition].reset_index()

    # create an identifier for each error instance.
    # In this case, the rule is checked for each CPPstartDate, in each CPplanDates group (differentiated by CP dates), in each child (differentiated by LAchildID)
    # So, a combination of LAchildID, PersonDeathDate identifies and error instance.
    df_merged["ERROR_ID"] = tuple(zip(df_merged[LAchildID], df_merged[PersonDeathDate]))

    # The merges were done on copies of cpp_df and reviews_df so that the column names in dataframes themselves aren't affected by the suffixes.
    # we can now map the suffixes columns to their corresponding source tables such that the failing ROW_IDs and ERROR_IDs exist per table.
    df_cpp_issues = (
        df_ci.merge(df_merged, left_on="ROW_ID", right_on="ROW_ID_ci")
        .groupby("ERROR_ID")["ROW_ID"]
        .apply(list)
        .reset_index()
    )
    df_reviews_issues = (
        df_cinplan.merge(df_merged, left_on="ROW_ID", right_on="ROW_ID_cinplan")
        .groupby("ERROR_ID")["ROW_ID"]
        .apply(list)
        .reset_index()
    )

    # Ensure that you maintain the ROW_ID, and ERROR_ID column names which are shown above. They are keywords in this project.
    rule_context.push_type_2(
        table=ChildIdentifiers, columns=[PersonDeathDate], row_df=df_cpp_issues
    )
    rule_context.push_type_2(
        table=CINplanDates, columns=[CINPlanEndDate], row_df=df_reviews_issues
    )


def test_validate():
    # Create some sample data such that some values pass the validation and some fail.
    sample_ci = pd.DataFrame(
        [
            {
                "LAchildID": "child1",
                "PersonDeathDate": "27/06/2002",  # Passes as dates are the same
                # "CIID": "cinID1",
            },
            {
                "LAchildID": "child2",
                "PersonDeathDate": "27/06/2002",  #  Fails, CINPlanEndDate greater than (27/6/2002) DeathDate
                # "CIID": "cinID2",
            },
            {
                "LAchildID": "child3",
                "PersonDeathDate": "01/01/2001",  # Fails as CINPlanEndDate greater than (01/01/2001) DeathDate
                # "CIID": "cinID3",
            },
            {
                "LAchildID": "child4",
                "PersonDeathDate": "01/01/2001",  # Passes as CINPlanEndDate is less than (01/01/2001) DeathDate
                # "CIID": "cinID4",
            },
            {
                "LAchildID": "child5",
                "PersonDeathDate": pd.NA,  # Ignored as rows with no PersonDeathDate are not picked up
                # "CIID": "cinID5",
            },
            {
                "LAchildID": "child6",
                "PersonDeathDate": "01/01/2008",  # Ignored as rows with no CINPlanEndDate are not picked up
                # "CIID": "cinID6",
            },
            {
                "LAchildID": "child7",
                "PersonDeathDate": "01/01/2008",  # Fails as CINPlanEndDate greater than (01/01/2008) DeathDate
                # "CIID": "cinID7",
            },
        ]
    )
    sample_cinplan = pd.DataFrame(
        [
            {
                "LAchildID": "child1",
                "CINPlanEndDate": "27/06/2002",  # Passes as dates are the same
                # "CIPID": "cinID1",
            },
            {
                "LAchildID": "child2",
                "CINPlanEndDate": "29/06/2002",  #  Fails, CINPlanEndDate greater than (27/6/2002) DeathDate
                # "CIPID": "cinID2",
            },
            {
                "LAchildID": "child3",
                "CINPlanEndDate": "07/02/2001",  # Fails as CINPlanEndDate greater than (01/01/2001) DeathDate
                # "CIPID": "cinID3",
            },
            {
                "LAchildID": "child4",
                "CINPlanEndDate": "26/05/2000",  # Passes as CINPlanEndDate is less than (01/01/2001) DeathDate
                # "CIPID": "cinID4",
            },
            {
                "LAchildID": "child5",
                "CINPlanEndDate": "26/05/2000",  # Ignored as rows with no PersonDeathDate are not picked up
                # "CIPID": "cinID5",
            },
            {
                "LAchildID": "child6",
                "CINPlanEndDate": pd.NA,  # Ignored as rows with no CINPlanEndDate are not picked up
                # "CIPID": "cinID6",
            },
            {
                "LAchildID": "child7",
                "CINPlanEndDate": "14/03/2008",  # Fails as CINPlanEndDate greater than (01/01/2008) DeathDate
                # "CIPID": "cinID7",
            },
        ]
    )

    # If rule requires columns containing date values, convert those columns to datetime objects first. Do it here in the test_validate function, not above.
    sample_ci[PersonDeathDate] = pd.to_datetime(
        sample_ci[PersonDeathDate], format="%d/%m/%Y", errors="coerce"
    )
    sample_cinplan["CINPlanEndDate"] = pd.to_datetime(
        sample_cinplan["CINPlanEndDate"], format="%d/%m/%Y", errors="coerce"
    )

    # Run the rule function, passing in our sample data.
    result = run_rule(
        validate,
        {
            ChildIdentifiers: sample_ci,
            CINplanDates: sample_cinplan,
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
                    "child2",  # ChildID
                    # CINPlanEndDate
                    pd.to_datetime("29/06/2002", format="%d/%m/%Y", errors="coerce"),
                ),
                "ROW_ID": [1],
            },
            {
                "ERROR_ID": (
                    "child3",  # ChildID
                    # CINPlanEndDate
                    pd.to_datetime("07/02/2001", format="%d/%m/%Y", errors="coerce"),
                ),
                "ROW_ID": [2],
            },
            {
                "ERROR_ID": (
                    "child7",  # ChildID
                    # CINPlanEndDate
                    pd.to_datetime("14/03/2008", format="%d/%m/%Y", errors="coerce"),
                ),
                "ROW_ID": [6],
            },
        ]
    )
    assert issue_rows.equals(expected_df)

    # Check that the rule definition is what you wrote in the context above.

    # replace 2885 with the rule code and put the appropriate message in its place too.
    assert result.definition.code == 4009
    assert (
        result.definition.message
        == "CIN Plan shown as ending after the child's Date of Death."
    )
