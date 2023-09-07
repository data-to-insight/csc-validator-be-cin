"""
Rule number: '1520'
Module: Child idenitifiers
Rule details: Each pupil <UPN> (N00001) must be unique across all pupils in the extract. 
Note: This rule should be evaluated at LA-level for imported data                                                                     
Rule message: More than one record with the same UPN
"""
from typing import Mapping

import pandas as pd

from cin_validator.rule_engine import CINTable, RuleContext, rule_definition
from cin_validator.test_engine import run_rule

ChildIdentifiers = CINTable.ChildIdentifiers
UPN = ChildIdentifiers.UPN


@rule_definition(
    code="1520",
    module=CINTable.ChildIdentifiers,
    message="More than one record with the same UPN.",
    affected_fields=[UPN],
)
def validate(
    data_container: Mapping[CINTable, pd.DataFrame], rule_context: RuleContext
):
    df = data_container[ChildIdentifiers]
    df.index.name = "ROW_ID"

    # Each pupil <UPN> (N00001) must be unique across all pupils in the extract

    df = df[df[UPN].notna()]
    df_issues = df[df.duplicated(subset=[UPN], keep=False)].reset_index()

    link_id = tuple(
        zip(
            df_issues["LAchildID"],
            df_issues[UPN],
        )
    )

    df_issues["ERROR_ID"] = link_id

    df_issues = (
        df_issues.groupby("ERROR_ID", group_keys=False)["ROW_ID"]
        .apply(list)
        .reset_index()
    )

    rule_context.push_type_1(table=ChildIdentifiers, columns=[UPN], row_df=df_issues)


def test_validate():
    child_identifiers = pd.DataFrame(
        [
            {
                "LAchildID": "child1",
                "UPN": "1234",
            },
            {
                "LAchildID": "child2",
                "UPN": "1234",
            },
            {
                "LAchildID": "child3",
                "UPN": "12345",
            },
            {
                "LAchildID": "child4",
                "UPN": pd.NA,
            },
            {
                "LAchildID": "child4",
                "UPN": pd.NA,
            },
        ]
    )

    result = run_rule(validate, {ChildIdentifiers: child_identifiers})

    # The result contains a NamedTuple of issues encountered
    issues = result.type1_issues

    # get table name and check it. Replace ChildProtectionPlans with the name of your table.
    issue_table = issues.table
    assert issue_table == ChildIdentifiers

    # check that the right columns were returned. Replace CPPstartDate and CPPendDate with a list of your columns.
    issue_columns = issues.columns
    assert issue_columns == [UPN]

    # check that the location linking dataframe was formed properly.
    issue_rows = issues.row_df

    # replace 2 with the number of failing points you expect from the sample data.
    assert len(issue_rows) == 2
    # replace the table and column name as done earlier.
    # The last numbers represent the index values where you expect the sample data to fail the validation check.
    # check that the failing locations are contained in a DataFrame having the appropriate columns. These lines do not change.
    assert isinstance(issue_rows, pd.DataFrame)
    assert issue_rows.columns.to_list() == ["ERROR_ID", "ROW_ID"]

    # The ROW ID values represent the index positions where you expect the sample data to fail the validation check.
    expected_df = pd.DataFrame(
        [
            {
                "ERROR_ID": (
                    "child1",
                    "1234",
                ),
                "ROW_ID": [0],
            },
            {
                "ERROR_ID": (
                    "child2",
                    "1234",
                ),
                "ROW_ID": [1],
            },
        ]
    )
    assert issue_rows.equals(expected_df)

    # Check that the rule definition is what you wrote in the context above.

    # replace 8840 with the rule code and put the appropriate message in its place too.
    assert result.definition.code == "1520"
    assert result.definition.message == "More than one record with the same UPN."
