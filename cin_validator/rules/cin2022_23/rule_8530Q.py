"""
Rule number: 8530Q
Module: Child Identifiers
Rule details: If present <ExpectedPersonBirthDate> (N00098) should be between [<ReferenceDate> (N00603) minus 30 days] and [<ReferenceDate> (N00603) plus 9 months]
Rule message: Please check: Expected Date of Birth is outside the expected range for this census (March to December of the Census Year end)
"""
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

ChildIdentifiers = CINTable.ChildIdentifiers
ExpectedPersonBirthDate = ChildIdentifiers.ExpectedPersonBirthDate
LAchildID = ChildIdentifiers.LAchildID

Header = CINTable.Header
ReferenceDate = Header.ReferenceDate


@rule_definition(
    code="8530Q",
    module=CINTable.ChildIdentifiers,
    rule_type=RuleType.QUERY,
    message="Please check and either amend data or provide a reason: Expected Date of Birth is outside the expected range for this census (March to December of the Census Year end)",
    affected_fields=[ExpectedPersonBirthDate],
)
def validate(
    data_container: Mapping[CINTable, pd.DataFrame], rule_context: RuleContext
):
    df = data_container[ChildIdentifiers]
    df.index.name = "ROW_ID"

    df_ref = data_container[Header]
    ref_date = df_ref[ReferenceDate].iloc[0]

    #  <ExpectedPersonBirthDate> (N00098) should be between [<ReferenceDate> (N00603) minus 30 days] and [<ReferenceDate> (N00603) plus 9 months]

    # Filter to only those with expected birthdate
    df = df[~df[ExpectedPersonBirthDate].isna()]

    # Find the reference date - 30
    earliest_date = ref_date - pd.DateOffset(days=30)
    # Find reference date + 9 months
    latest_date = ref_date + pd.DateOffset(months=9)

    condition1 = df[ExpectedPersonBirthDate] >= latest_date
    condition2 = df[ExpectedPersonBirthDate] <= earliest_date

    df_issues = df[condition1 | condition2].reset_index()

    link_id = tuple(zip(df_issues[LAchildID], df_issues[ExpectedPersonBirthDate]))
    df_issues["ERROR_ID"] = link_id
    df_issues = (
        df_issues.groupby("ERROR_ID", group_keys=False)["ROW_ID"]
        .apply(list)
        .reset_index()
    )

    rule_context.push_type_2(
        table=ChildIdentifiers,
        columns=[ExpectedPersonBirthDate],
        row_df=df_issues,
    )


def test_validate():
    sample_ChildIdentifiers = pd.DataFrame(
        [
            {
                "LAchildID": "ID1",
                "ExpectedPersonBirthDate": pd.NA,
                # Pass, no birth date
            },
            {
                "LAchildID": "ID2",
                "ExpectedPersonBirthDate": "30/03/2023",
                # Pass, birth date within range
            },
            {
                "LAchildID": "ID3",
                "ExpectedPersonBirthDate": "15/10/2021",
                # Fail, start date is before ref date - 45wd
            },
            {
                "LAchildID": "ID4",
                "ExpectedPersonBirthDate": "15/03/2024",
                # Fail, later than 9 months after ref date
            },
        ]
    )

    sample_ChildIdentifiers[ExpectedPersonBirthDate] = pd.to_datetime(
        sample_ChildIdentifiers[ExpectedPersonBirthDate],
        format="%d/%m/%Y",
        errors="coerce",
    )

    sample_header = pd.DataFrame([{"ReferenceDate": "31/03/2023"}])

    sample_header[ReferenceDate] = pd.to_datetime(
        sample_header[ReferenceDate], format="%d/%m/%Y", errors="coerce"
    )

    result = run_rule(
        validate,
        {
            ChildIdentifiers: sample_ChildIdentifiers,
            Header: sample_header,
        },
    )

    issues_list = result.type2_issues
    assert len(issues_list) == 1
    issues = issues_list[0]

    issue_table = issues.table
    assert issue_table == ChildIdentifiers

    issue_columns = issues.columns
    assert issue_columns == [ExpectedPersonBirthDate]

    issue_rows = issues.row_df
    assert len(issue_rows) == 2
    assert isinstance(issue_rows, pd.DataFrame)
    assert issue_rows.columns.to_list() == ["ERROR_ID", "ROW_ID"]

    expected_df = pd.DataFrame(
        [
            {
                "ERROR_ID": (
                    "ID3",
                    pd.to_datetime("15/10/2021", format="%d/%m/%Y", errors="coerce"),
                ),
                "ROW_ID": [2],
            },
            {
                "ERROR_ID": (
                    "ID4",
                    pd.to_datetime("15/03/2024", format="%d/%m/%Y", errors="coerce"),
                ),
                "ROW_ID": [3],
            },
        ]
    )

    assert issue_rows.equals(expected_df)

    assert result.definition.code == "8530Q"
    assert (
        result.definition.message
        == "Please check and either amend data or provide a reason: Expected Date of Birth is outside the expected range for this census (March to December of the Census Year end)"
    )
