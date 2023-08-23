from typing import Mapping

import pandas as pd

from cin_validator.rule_engine import CINTable, RuleContext, RuleType, rule_definition
from cin_validator.test_engine import run_rule

ChildIdentifiers = CINTable.ChildIdentifiers
CINplanDates = CINTable.CINplanDates

LAchildID = ChildIdentifiers.LAchildID
PersonDeathDate = ChildIdentifiers.PersonDeathDate
CINPlanEndDate = CINplanDates.CINPlanEndDate


@rule_definition(
    code="4009Q",
    module=CINTable.ChildIdentifiers,
    rule_type=RuleType.QUERY,
    message="CIN Plan cannot end after the child’s Date of Death",
    affected_fields=[
        PersonDeathDate,
        CINPlanEndDate,
    ],
)
def validate(
    data_container: Mapping[CINTable, pd.DataFrame], rule_context: RuleContext
):
    df_ci = data_container[ChildIdentifiers].copy()
    df_cin = data_container[CINplanDates].copy()

    df_ci.index.name = "ROW_ID"
    df_cin.index.name = "ROW_ID"

    df_ci.reset_index(inplace=True)
    df_cin.reset_index(inplace=True)

    # If <PersonDeathDate> (N00108) is present, then <CINPlanEndDate> (N00690) must be on or before <PersonDeathDate> (N00108)
    df_ci = df_ci[df_ci["PersonDeathDate"].notna()]

    merged_df = df_ci.merge(
        df_cin,
        on=[
            LAchildID,
        ],
        suffixes=["_ci", "_cin"],
    )

    condition = merged_df[PersonDeathDate] < merged_df[CINPlanEndDate]

    merged_df = merged_df[condition].reset_index()

    merged_df["ERROR_ID"] = tuple(
        zip(
            merged_df[LAchildID],
            merged_df[PersonDeathDate],
        )
    )

    df_ci_issues = (
        df_ci.merge(merged_df, left_on="ROW_ID", right_on="ROW_ID_ci")
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

    rule_context.push_type_2(
        table=ChildIdentifiers, columns=[PersonDeathDate], row_df=df_ci_issues
    )
    rule_context.push_type_2(
        table=CINplanDates, columns=[CINPlanEndDate], row_df=df_cin_issues
    )


def test_validate():
    sample_ci = pd.DataFrame(
        [
            {
                "LAchildID": "child1",  # Fail
                "PersonDeathDate": "01/01/2000",
            },
            {
                "LAchildID": "child2",  # Pass
                "PersonDeathDate": "01/01/2000",
            },
            {
                "LAchildID": "child3",  # Pass
                "PersonDeathDate": pd.NA,
            },
            {
                "LAchildID": "child4",  # Fail
                "PersonDeathDate": "01/01/2000",
            },
        ]
    )
    sample_cin = pd.DataFrame(
        [
            {
                "LAchildID": "child1",
                "CINPlanEndDate": "01/01/2001",
            },
            {
                "LAchildID": "child2",
                "CINPlanEndDate": pd.NA,
            },
            {
                "LAchildID": "child3",
                "CINPlanEndDate": "01/01/2000",
            },
            {
                "LAchildID": "child4",
                "CINPlanEndDate": "01/01/2001",
            },
        ]
    )
    # if rule requires columns containing date values, convert those columns to datetime objects first. Do it here in the test_validate function, not above.
    sample_ci[PersonDeathDate] = pd.to_datetime(
        sample_ci[PersonDeathDate], format="%d/%m/%Y", errors="coerce"
    )

    sample_cin["CINPlanEndDate"] = pd.to_datetime(
        sample_cin["CINPlanEndDate"], format="%d/%m/%Y", errors="coerce"
    )

    # Run the rule function, passing in our sample data.
    result = run_rule(
        validate,
        {
            ChildIdentifiers: sample_ci,
            CINplanDates: sample_cin,
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
    assert issue_table == ChildIdentifiers

    # check that the right columns were returned. Replace DateOfInitialCPC  with a list of your columns.
    issue_columns = issues.columns
    assert issue_columns == [PersonDeathDate]

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
                    pd.to_datetime("01/01/2000", format="%d/%m/%Y", errors="coerce"),
                ),
                "ROW_ID": [0],
            },
            {
                "ERROR_ID": (
                    "child4",
                    pd.to_datetime("01/01/2000", format="%d/%m/%Y", errors="coerce"),
                ),
                "ROW_ID": [3],
            },
        ]
    )
    assert issue_rows.equals(expected_df)

    # Check that the rule definition is what you wrote in the context above.

    # replace 4009Q with the rule code and put the appropriate message in its place too.
    assert result.definition.code == "4009Q"
    assert (
        result.definition.message
        == "CIN Plan cannot end after the child’s Date of Death"
    )
