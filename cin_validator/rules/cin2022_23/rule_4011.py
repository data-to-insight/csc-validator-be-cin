from typing import Mapping

import pandas as pd

from cin_validator.rule_engine import CINTable, RuleContext, rule_definition
from cin_validator.test_engine import run_rule

CINplanDates = CINTable.CINplanDates
CINPlanStartDate = CINplanDates.CINPlanStartDate
CINPlanEndDate = CINplanDates.CINPlanEndDate
LAchildID = CINplanDates.LAchildID


@rule_definition(
    code="4011",
    module=CINTable.CINplanDates,
    message="CIN Plan End Date earlier than Start Date",
    affected_fields=[CINPlanEndDate, CINPlanStartDate],
)
def validate(
    data_container: Mapping[CINTable, pd.DataFrame], rule_context: RuleContext
):
    df = data_container[CINplanDates]
    df.index.name = "ROW_ID"

    # <If present <CINPlanEndDate> (N00690) must be on or after the <CINPlanStartDate> (N00689)
    # Remove all rows with no end date
    df = df[~df[CINPlanEndDate].isna()]

    # Return rows where end date is prior to start dat
    condition1 = df[CINPlanEndDate] < df[CINPlanStartDate]

    # df with all rows meeting the conditions
    df_issues = df[condition1].reset_index()

    link_id = tuple(
        zip(
            df_issues[LAchildID], df_issues[CINPlanEndDate], df_issues[CINPlanStartDate]
        )
    )
    df_issues["ERROR_ID"] = link_id
    df_issues = (
        df_issues.groupby("ERROR_ID", group_keys=False)["ROW_ID"]
        .apply(list)
        .reset_index()
    )

    rule_context.push_type_1(
        table=CINplanDates, columns=[CINPlanEndDate, CINPlanStartDate], row_df=df_issues
    )


def test_validate():
    cin_plan = pd.DataFrame(
        [
            {
                "LAchildID": "child1",
                "CINPlanEndDate": "26/05/2000",
                "CINPlanStartDate": "26/05/2000",
            },
            {
                "LAchildID": "child2",
                "CINPlanEndDate": "26/05/2000",
                "CINPlanStartDate": "26/05/2001",
                # fails, start after end
            },
            {
                "LAchildID": "child3",
                "CINPlanEndDate": "26/05/2000",
                "CINPlanStartDate": "26/05/1999",
            },
            {
                "LAchildID": "child4",
                "CINPlanEndDate": "26/05/2000",
                "CINPlanStartDate": pd.NA,
                # pass, no requirement in this rule for a start date
            },
            {
                "LAchildID": "child6",
                "CINPlanEndDate": pd.NA,
                "CINPlanStartDate": pd.NA,
            },
        ]
    )
    cin_plan[CINPlanEndDate] = pd.to_datetime(
        cin_plan[CINPlanEndDate], format="%d/%m/%Y", errors="coerce"
    )
    cin_plan[CINPlanStartDate] = pd.to_datetime(
        cin_plan[CINPlanStartDate], format="%d/%m/%Y", errors="coerce"
    )

    result = run_rule(validate, {CINplanDates: cin_plan})

    issues = result.type1_issues

    issue_table = issues.table
    assert issue_table == CINplanDates

    issue_columns = issues.columns
    assert issue_columns == [CINPlanEndDate, CINPlanStartDate]

    issue_rows = issues.row_df
    assert len(issue_rows) == 1
    assert isinstance(issue_rows, pd.DataFrame)
    assert issue_rows.columns.to_list() == ["ERROR_ID", "ROW_ID"]

    expected_df = pd.DataFrame(
        [
            {
                "ERROR_ID": (
                    "child2",
                    pd.to_datetime("26/05/2000", format="%d/%m/%Y", errors="coerce"),
                    pd.to_datetime("26/05/2001", format="%d/%m/%Y", errors="coerce"),
                ),
                "ROW_ID": [1],
            },
        ]
    )
    assert issue_rows.equals(expected_df)

    assert result.definition.code == "4011"
    assert result.definition.message == "CIN Plan End Date earlier than Start Date"
