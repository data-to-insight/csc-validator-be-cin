from typing import Mapping

import pandas as pd

from cin_validator.rule_engine import (
    CINTable,
    IssueLocator,
    RuleContext,
    rule_definition,
)
from cin_validator.test_engine import run_rule

Header = CINTable.Header
ReferenceDate = Header.ReferenceDate
Year = Header.Year


@rule_definition(
    code="100",
    module=CINTable.Header,
    message="Reference Date is incorrect",
    affected_fields=[ReferenceDate],
)
def validate(
    data_container: Mapping[CINTable, pd.DataFrame], rule_context: RuleContext
):
    df = data_container[Header]

    # <ReferenceDate> (N00603) must be present and must equal 2022-03-31

    # ReferenceDate is expected to be the 31st of March in the collection year
    df["expected_date"] = "31/03/" + df[Year]
    df["expected_date"] = pd.to_datetime(
        df["expected_date"], format="%d/%m/%Y", errors="coerce"
    )

    # Checks that the reference date is present
    is_present = df[ReferenceDate].isna()
    # Checks the error date is equal to 31/03/[collection_year].
    error_date = df[ReferenceDate] != df["expected_date"]
    failing_indices = df[is_present | error_date].index

    rule_context.push_issue(table=Header, field=ReferenceDate, row=failing_indices)


def test_validate():
    # Create some sample data such that some values pass the validation and some fail.
    # Sidenote: a typical Header table will only have one row.
    header = pd.DataFrame(
        [
            {Year: "2022", ReferenceDate: "31/03/2022"},
            {Year: "2022", ReferenceDate: pd.NA},
            {Year: "2022", ReferenceDate: pd.NA},
            {Year: "2022", ReferenceDate: "30/11/2021"},
        ]
    )

    header[ReferenceDate] = pd.to_datetime(
        header[ReferenceDate], format="%d/%m/%Y", errors="coerce"
    )

    result = run_rule(validate, {Header: header})

    issues = list(result.issues)
    # Intended fail points in data.
    assert len(issues) == 3
    # Intended failures of test data by index.
    assert issues == [
        IssueLocator(CINTable.Header, ReferenceDate, 1),
        IssueLocator(CINTable.Header, ReferenceDate, 2),
        IssueLocator(CINTable.Header, ReferenceDate, 3),
    ]

    # Checks rule code and message are correct.
    assert result.definition.code == "100"
    assert result.definition.message == "Reference Date is incorrect"
