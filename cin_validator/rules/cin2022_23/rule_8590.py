from typing import Mapping

import pandas as pd

from cin_validator.rule_engine import CINTable, RuleContext, rule_definition
from cin_validator.test_engine import run_rule

ChildIdentifiers = CINTable.ChildIdentifiers
CINdetails = CINTable.CINdetails

LAchildID = ChildIdentifiers.LAchildID


@rule_definition(
    code="8590",
    module=CINTable.ChildIdentifiers,
    message="Child does not have a recorded CIN episode.",
    affected_fields=[LAchildID],
)
def validate(
    data_container: Mapping[CINTable, pd.DataFrame], rule_context: RuleContext
):
    df_cid = data_container[ChildIdentifiers].copy()
    df_cin = data_container[CINdetails].copy()

    df_cid.index.name = "ROW_ID"
    df_cin.index.name = "ROW_ID"

    df_cid.reset_index(inplace=True)
    df_cin.reset_index(inplace=True)

    # Each child must have at least one <CINdetails> group
    df_merge = df_cid.merge(
        df_cin[LAchildID], on=[LAchildID], how="left", indicator=True
    )

    condition = df_merge["_merge"] == "left_only"

    df_merge = df_merge[condition].reset_index()

    df_merge["ERROR_ID"] = tuple(zip(df_merge[LAchildID]))

    df_cid_issues = (
        df_cid.merge(df_merge, left_on="ROW_ID", right_on="ROW_ID")
        .groupby("ERROR_ID", group_keys=False)["ROW_ID"]
        .apply(list)
        .reset_index()
    )

    rule_context.push_type_2(
        table=ChildIdentifiers, columns=[LAchildID], row_df=df_cid_issues
    )


def test_validate():
    sample_child_identifiers = pd.DataFrame(
        [
            {
                "LAchildID": "child1",  # Pass
            },
            {
                "LAchildID": "child2",  # Pass
            },
            {
                "LAchildID": "child3",  # Fail
            },
            {
                "LAchildID": "child4",  # Pass
            },
        ]
    )
    sample_cin_details = pd.DataFrame(
        [
            {
                "LAchildID": "child1",  # Pass
            },
            {
                "LAchildID": "child2",  # Pass
            },
            {
                "LAchildID": "child4",  # Pass
            },
            {
                "LAchildID": "child5",  # Ignore
            },
        ]
    )

    result = run_rule(
        validate,
        {
            ChildIdentifiers: sample_child_identifiers,
            CINdetails: sample_cin_details,
        },
    )

    issues_list = result.type2_issues
    assert len(issues_list) == 1
    issues = issues_list[0]

    issue_table = issues.table
    assert issue_table == ChildIdentifiers

    issue_columns = issues.columns
    assert issue_columns == [LAchildID]

    issue_rows = issues.row_df
    assert len(issue_rows) == 1

    assert isinstance(issue_rows, pd.DataFrame)
    assert issue_rows.columns.to_list() == ["ERROR_ID", "ROW_ID"]

    expected_df = pd.DataFrame(
        [
            {
                "ERROR_ID": ("child3",),  # ChildID
                "ROW_ID": [2],
            },
        ]
    )
    assert issue_rows.equals(expected_df)

    assert result.definition.code == "8590"
    assert result.definition.message == "Child does not have a recorded CIN episode."
