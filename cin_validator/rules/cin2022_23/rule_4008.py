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
LAchildID = CINplanDates.LAchildID
CINPlanStartDate = CINplanDates.CINPlanStartDate

# define characteristics of rule
@rule_definition(
    # write the rule code here
    code=4008,
    # replace ChildProtectionPlans with the value in the module column of the excel sheet corresponding to this rule .
    # Note that even if multiple tables are involved, one table will be named in the module column.
    module=CINTable.ChildIdentifiers,
    # replace the message with the corresponding value for this rule, gotten from the excel sheet.
    message="CIN Plan shown as starting after the child’s Date of Death.",
    # The column names tend to be the words within the < > signs in the github issue description.
    affected_fields=[
        PersonDeathDate,
        CINPlanStartDate,
    ],
)
def validate(
    data_container: Mapping[CINTable, pd.DataFrame], rule_context: RuleContext
):
    # PREPARING DATA

    # Replace ChildProtectionPlans with the name of the table you need.
    df_ci = data_container[ChildIdentifiers].copy()
    df_cpd = data_container[CINplanDates].copy()

    # Before you begin, rename the index so that the initial row positions can be kept intact.
    df_ci.index.name = "ROW_ID"
    df_cpd.index.name = "ROW_ID"

    # Resetting the index causes the ROW_IDs to become columns of their respective DataFrames
    # so that they can come along when the merge is done.
    df_ci.reset_index(inplace=True)
    df_cpd.reset_index(inplace=True)

    # lOGIC
    # Implement rule logic as described by the Github issue.
    # Put the description as a comment above the implementation as shown.

    # If <PersonDeathDate> (N00108) is present, then <CINPlanStartDate> (N00689) must be on or before <PersonDeathDate> (N00108)

    df_ci = df_ci[df_ci[PersonDeathDate].notna()]
    df_cpd = df_cpd[df_cpd[CINPlanStartDate].notna()]

    #  Merge tables to get corresponding CP plan group and reviews
    df_merged = df_ci.merge(
        df_cpd,
        left_on=["LAchildID"],
        right_on=["LAchildID"],
        how="left",
        suffixes=("_ci", "_cpd"),
    )

    #  Get rows where PersonDeathDate is earlier than to CINPlanStartDate
    condition = df_merged[PersonDeathDate] < df_merged[CINPlanStartDate]
    df_merged = df_merged[condition].reset_index()

    # create an identifier for each error instance.
    # In this case, the rule is checked for each CPPstartDate, in each CPplanDates group (differentiated by CP dates), in each child (differentiated by LAchildID)
    # So, a combination of LAchildID, CPPstartDate and CPPreviewDate identifies and error instance.
    df_merged["ERROR_ID"] = tuple(
        zip(
            df_merged[LAchildID],
            df_merged[PersonDeathDate],
        )
    )

    # The merges were done on copies of cpp_df and reviews_df so that the column names in dataframes themselves aren't affected by the suffixes.
    # we can now map the suffixes columns to their corresponding source tables such that the failing ROW_IDs and ERROR_IDs exist per table.
    df_cpp_issues = (
        df_ci.merge(df_merged, left_on="ROW_ID", right_on="ROW_ID_ci")
        .groupby("ERROR_ID", group_keys=False)["ROW_ID"]
        .apply(list)
        .reset_index()
    )
    df_reviews_issues = (
        df_cpd.merge(df_merged, left_on="ROW_ID", right_on="ROW_ID_cpd")
        .groupby("ERROR_ID", group_keys=False)["ROW_ID"]
        .apply(list)
        .reset_index()
    )

    # Ensure that you maintain the ROW_ID, and ERROR_ID column names which are shown above. They are keywords in this project.
    rule_context.push_type_2(
        table=ChildIdentifiers, columns=[PersonDeathDate], row_df=df_cpp_issues
    )
    rule_context.push_type_2(
        table=CINplanDates, columns=[CINPlanStartDate], row_df=df_reviews_issues
    )


def test_validate():
    # Create some sample data such that some values pass the validation and some fail.
    sample_ci = pd.DataFrame(
        [
            {
                "LAchildID": "child1",
                "PersonDeathDate": "26/05/2000",  # Passes same date
            },
            {
                "LAchildID": "child2",
                "PersonDeathDate": "27/06/2002",  # Passes
            },
            {
                "LAchildID": "child3",
                "PersonDeathDate": "07/02/2001",  # Passes
            },
            {
                "LAchildID": "child4",
                "PersonDeathDate": "26/05/2000",  # Passes
            },
            {
                "LAchildID": "child5",
                "PersonDeathDate": "26/05/2000",  # Fails, death before CIN plan starts
            },
            {
                "LAchildID": "child6",
                "PersonDeathDate": pd.NA,  # Passes
            },
            {
                "LAchildID": "child7",
                "PersonDeathDate": "14/03/2001",  # Passes
            },
        ]
    )
    sample_cpd = pd.DataFrame(
        [
            {
                "LAchildID": "child1",
                "CINPlanStartDate": "26/05/2000",
            },
            {
                "LAchildID": "child2",
                "CINPlanStartDate": "26/05/2000",
            },
            {
                "LAchildID": "child3",
                "CINPlanStartDate": "26/05/2000",
            },
            {
                "LAchildID": "child4",
                "CINPlanStartDate": "25/05/2000",
            },
            {
                "LAchildID": "child5",
                "CINPlanStartDate": "27/05/2000",
            },
            {
                "LAchildID": "child6",
                "CINPlanStartDate": "26/05/2000",
            },
            {
                "LAchildID": "child7",
                "CINPlanStartDate": pd.NA,
            },
        ]
    )

    # If rule requires columns containing date values, convert those columns to datetime objects first. Do it here in the test_validate function, not above.
    sample_ci["PersonDeathDate"] = pd.to_datetime(
        sample_ci["PersonDeathDate"], format="%d/%m/%Y", errors="coerce"
    )
    sample_cpd["CINPlanStartDate"] = pd.to_datetime(
        sample_cpd["CINPlanStartDate"], format="%d/%m/%Y", errors="coerce"
    )

    # Run the rule function, passing in our sample data.
    result = run_rule(
        validate,
        {
            ChildIdentifiers: sample_ci,
            CINplanDates: sample_cpd,
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
    assert issue_columns == [CINPlanStartDate]

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
                    "child5",
                    pd.to_datetime("26/05/2000", format="%d/%m/%Y", errors="coerce"),
                ),
                "ROW_ID": [4],
            },
        ]
    )
    assert issue_rows.equals(expected_df)

    # Check that the rule definition is what you wrote in the context above.

    # replace 2885 with the rule code and put the appropriate message in its place too.
    assert result.definition.code == 4008
    assert (
        result.definition.message
        == "CIN Plan shown as starting after the child’s Date of Death."
    )
