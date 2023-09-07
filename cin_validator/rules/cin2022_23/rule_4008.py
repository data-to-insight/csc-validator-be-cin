from typing import Mapping

import pandas as pd

from cin_validator.rule_engine import CINTable, RuleContext, rule_definition
from cin_validator.test_engine import run_rule

ChildIdentifiers = CINTable.ChildIdentifiers
PersonDeathDate = ChildIdentifiers.PersonDeathDate
LAchildID = ChildIdentifiers.LAchildID

CINplanDates = CINTable.CINplanDates
LAchildID = CINplanDates.LAchildID
CINPlanStartDate = CINplanDates.CINPlanStartDate


@rule_definition(
    code="4008",
    module=CINTable.ChildIdentifiers,
    message="CIN Plan shown as starting after the child’s Date of Death.",
    affected_fields=[
        PersonDeathDate,
        CINPlanStartDate,
    ],
)
def validate(
    data_container: Mapping[CINTable, pd.DataFrame], rule_context: RuleContext
):
    df_ci = data_container[ChildIdentifiers].copy()
    df_cpd = data_container[CINplanDates].copy()

    df_ci.index.name = "ROW_ID"
    df_cpd.index.name = "ROW_ID"

    df_ci.reset_index(inplace=True)
    df_cpd.reset_index(inplace=True)

    # If <PersonDeathDate> (N00108) is present, then <CINPlanStartDate> (N00689) must be on or before <PersonDeathDate> (N00108)
    df_ci = df_ci[df_ci[PersonDeathDate].notna()]
    df_cpd = df_cpd[df_cpd[CINPlanStartDate].notna()]

    df_merged = df_ci.merge(
        df_cpd,
        left_on=["LAchildID"],
        right_on=["LAchildID"],
        how="left",
        suffixes=("_ci", "_cpd"),
    )

    condition = df_merged[PersonDeathDate] < df_merged[CINPlanStartDate]
    df_merged = df_merged[condition].reset_index()

    df_merged["ERROR_ID"] = tuple(
        zip(
            df_merged[LAchildID],
            df_merged[PersonDeathDate],
        )
    )

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

    rule_context.push_type_2(
        table=ChildIdentifiers, columns=[PersonDeathDate], row_df=df_cpp_issues
    )
    rule_context.push_type_2(
        table=CINplanDates, columns=[CINPlanStartDate], row_df=df_reviews_issues
    )


def test_validate():
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

    sample_ci["PersonDeathDate"] = pd.to_datetime(
        sample_ci["PersonDeathDate"], format="%d/%m/%Y", errors="coerce"
    )
    sample_cpd["CINPlanStartDate"] = pd.to_datetime(
        sample_cpd["CINPlanStartDate"], format="%d/%m/%Y", errors="coerce"
    )

    result = run_rule(
        validate,
        {
            ChildIdentifiers: sample_ci,
            CINplanDates: sample_cpd,
        },
    )

    issues_list = result.type2_issues
    assert len(issues_list) == 2
    issues = issues_list[1]

    issue_table = issues.table
    assert issue_table == CINplanDates

    issue_columns = issues.columns
    assert issue_columns == [CINPlanStartDate]

    issue_rows = issues.row_df
    assert len(issue_rows) == 1
    assert isinstance(issue_rows, pd.DataFrame)
    assert issue_rows.columns.to_list() == ["ERROR_ID", "ROW_ID"]

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

    assert result.definition.code == "4008"
    assert (
        result.definition.message
        == "CIN Plan shown as starting after the child’s Date of Death."
    )
