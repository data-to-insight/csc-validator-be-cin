"""
Rule number: '4004'
Module: CIN plan dates
Rule details: Within a <CINDetails> module, there must be only one <CINplanDates> group where the <CINPlanEnd Date> (N00690) is missing
Rule message: This child is showing more than one open CIN Plan, i.e. with no End Date

"""

from typing import Mapping

import pandas as pd

from cin_validator.rule_engine import CINTable, RuleContext, rule_definition
from cin_validator.rules.cin2022_23.rule_8925 import LAchildID
from cin_validator.test_engine import run_rule

CINplanDates = CINTable.CINplanDates
LAchildID = CINplanDates.LAchildID
CINPlanEndDate = CINplanDates.CINPlanEndDate
CINdetailsID = CINplanDates.CINdetailsID


@rule_definition(
    code="4004",
    module=CINTable.CINplanDates,
    message="This child is showing more than one open CIN Plan, i.e. with no End Date",
    affected_fields=[CINPlanEndDate],
)
def validate(
    data_container: Mapping[CINTable, pd.DataFrame], rule_context: RuleContext
):
    df = data_container[CINplanDates]
    df.index.name = "ROW_ID"
    df.reset_index(inplace=True)

    # There must be only one <CINplanDates> group where the <CINPlanEnd Date> (N00690) is missing
    df_check = df.copy()
    df_check = df_check[df_check[CINPlanEndDate].isna()]

    # Convert NAs to 1 and count by child
    df_check[CINPlanEndDate].fillna(1, inplace=True)
    df_check = (
        df_check.groupby([LAchildID, CINdetailsID])[CINPlanEndDate]
        .count()
        .reset_index()
    )

    # Find where there is more than 1 open end date
    df_check = df_check[df_check[CINPlanEndDate] > 1]
    issue_ids = tuple(zip(df_check[LAchildID], df_check[CINdetailsID]))

    df["ERROR_ID"] = tuple(zip(df[LAchildID], df[CINdetailsID]))
    df_issues = df[df.ERROR_ID.isin(issue_ids)]

    df_issues = (
        df_issues.groupby("ERROR_ID", group_keys=False)["ROW_ID"]
        .apply(list)
        .reset_index()
    )
    rule_context.push_type_3(
        table=CINplanDates, columns=[CINPlanEndDate], row_df=df_issues
    )


def test_validate():
    sample_CINplanDates = pd.DataFrame(
        [  # child1
            {
                LAchildID: "child1",
                CINdetailsID: "CINdetailsID1",
                CINPlanEndDate: pd.NA,  # 0 first nan date in group
            },
            {
                LAchildID: "child1",
                CINdetailsID: "CINdetailsID1",
                CINPlanEndDate: pd.NA,  # 1 second nan date in group
            },
            {  # won't be flagged because there is not more than one nan authorisation date in this group.
                LAchildID: "child1",
                CINdetailsID: "CINdetailsID2",
                CINPlanEndDate: pd.NA,  # 2
            },
            # child2
            {
                LAchildID: "child2",
                CINdetailsID: "CINdetailsID1",
                CINPlanEndDate: "26/05/2021",  # 3 ignored. not nan
            },
            {  # fail
                LAchildID: "child2",
                CINdetailsID: "CINdetailsID2",
                CINPlanEndDate: pd.NA,  # 4 first nan date in group
            },
            {  # fail
                LAchildID: "child2",
                CINdetailsID: "CINdetailsID2",
                CINPlanEndDate: pd.NA,  # 5 second nan date in group
            },
        ]
    )
    sample_CINplanDates[CINPlanEndDate] = pd.to_datetime(
        sample_CINplanDates[CINPlanEndDate],
        format="%d/%m/%Y",
        errors="coerce",
    )

    result = run_rule(validate, {CINplanDates: sample_CINplanDates})

    issues_list = result.type3_issues

    assert len(issues_list) == 1
    issues = issues_list[0]

    issue_table = issues.table
    assert issue_table == CINplanDates

    issue_columns = issues.columns
    assert issue_columns == [CINPlanEndDate]

    issue_rows = issues.row_df
    assert len(issue_rows) == 2
    assert isinstance(issue_rows, pd.DataFrame)
    assert issue_rows.columns.to_list() == ["ERROR_ID", "ROW_ID"]

    expected_df = pd.DataFrame(
        [
            {
                "ERROR_ID": (
                    "child1",
                    "CINdetailsID1",
                ),
                "ROW_ID": [0, 1],
            },
            {
                "ERROR_ID": (
                    "child2",
                    "CINdetailsID2",
                ),
                "ROW_ID": [4, 5],
            },
        ]
    )
    assert issue_rows.equals(expected_df)

    assert result.definition.code == "4004"
    assert (
        result.definition.message
        == "This child is showing more than one open CIN Plan, i.e. with no End Date"
    )
