from typing import Mapping

import pandas as pd

from cin_validator.rule_engine import (
    CINTable,
    IssueLocator,
    RuleContext,
    RuleType,
    rule_definition,
)
from cin_validator.test_engine import run_rule

CINplanDates = CINTable.CINplanDates
CINPlanStartDate = CINplanDates.CINPlanStartDate
CINPlanEndDate = CINplanDates.CINPlanEndDate
LAchildID = CINplanDates.LAchildID


@rule_definition(
    code="4012Q",
    rule_type=RuleType.QUERY,
    module=CINTable.CINplanDates,
    message="Please check and either amend or provide a reason: CIN Plan shown as starting and ending on the same day",
    affected_fields=[CINPlanStartDate, CINPlanEndDate],
)
def validate(
    data_container: Mapping[CINTable, pd.DataFrame], rule_context: RuleContext
):
    df = data_container[CINplanDates]

    # Within a <CINPlanDates> group, <CINPlanStartDate> (N00689) should not be the same as the <CINPlanEndDate> (N00690)
    df.index.name = "ROW_ID"
    df = df[df["CINPlanStartDate"] == df["CINPlanEndDate"]]

    df_issues = df.reset_index()

    link_id = tuple(
        zip(
            df_issues[LAchildID], df_issues[CINPlanStartDate], df_issues[CINPlanEndDate]
        )
    )
    df_issues["ERROR_ID"] = link_id
    df_issues = (
        df_issues.groupby("ERROR_ID", group_keys=False)["ROW_ID"]
        .apply(list)
        .reset_index()
    )
    rule_context.push_type_1(
        table=CINplanDates, columns=[CINPlanStartDate, CINPlanEndDate], row_df=df_issues
    )


def test_validate():
    IDS_are = [
        "AAAAAAAA",
        "BBBBBBBBB",
        "CCCCCCCCCCC",
        "DDDDDDDDD",
        "EEEE",
        "FFFFFFFFF",
        "GGGGGGGGGG",
        "HHHH",
    ]
    starts = [
        "01-01-2020",
        "01-02-2020",
        "01-03-2020",
        "15-01-2020",
        pd.NA,
        "01-07-2020",
        "15-01-2020",
        pd.NA,
    ]
    ends = [
        "01-01-2020",
        "01-01-2020",
        "01-03-2020",
        "17-01-2020",
        pd.NA,
        "01-01-2020",
        "15-01-2020",
        "01-01-2020",
    ]
    #  Fails rows 0, 2, 6.
    fake_dataframe = pd.DataFrame(
        {"LAchildID": IDS_are, "CINPlanStartDate": starts, "CINPlanEndDate": ends}
    )

    fake_dataframe[CINPlanStartDate] = pd.to_datetime(
        fake_dataframe[CINPlanStartDate], format=r"%d-%m-%Y", errors="coerce"
    )
    fake_dataframe[CINPlanEndDate] = pd.to_datetime(
        fake_dataframe[CINPlanEndDate], format=r"%d-%m-%Y", errors="coerce"
    )

    result = run_rule(validate, {CINplanDates: fake_dataframe})

    issues = result.type1_issues

    issue_table = issues.table
    assert issue_table == CINplanDates

    issue_columns = issues.columns
    assert issue_columns == [CINPlanStartDate, CINPlanEndDate]

    issue_rows = issues.row_df
    assert len(issue_rows) == 3
    assert isinstance(issue_rows, pd.DataFrame)
    assert issue_rows.columns.to_list() == ["ERROR_ID", "ROW_ID"]

    expected_df = pd.DataFrame(
        [
            {
                "ERROR_ID": (
                    "AAAAAAAA",
                    pd.to_datetime("01-01-2020", format=r"%d-%m-%Y", errors="coerce"),
                    pd.to_datetime("01-01-2020", format=r"%d-%m-%Y", errors="coerce"),
                ),
                "ROW_ID": [0],
            },
            {
                "ERROR_ID": (
                    "CCCCCCCCCCC",
                    pd.to_datetime("01-03-2020", format=r"%d-%m-%Y", errors="coerce"),
                    pd.to_datetime("01-03-2020", format=r"%d-%m-%Y", errors="coerce"),
                ),
                "ROW_ID": [2],
            },
            {
                "ERROR_ID": (
                    "GGGGGGGGGG",
                    pd.to_datetime("15-01-2020", format=r"%d-%m-%Y", errors="coerce"),
                    pd.to_datetime("15-01-2020", format=r"%d-%m-%Y", errors="coerce"),
                ),
                "ROW_ID": [6],
            },
        ]
    )
    assert issue_rows.equals(expected_df)

    assert result.definition.code == "4012Q"
    assert (
        result.definition.message
        == "Please check and either amend or provide a reason: CIN Plan shown as starting and ending on the same day"
    )
