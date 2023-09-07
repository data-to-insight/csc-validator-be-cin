"""
Rule number: '8875'
Module: Section 47
Rule details: Where present, the <DateOfInitialCPC> (N00110) must not be a Saturday, Sunday
Rule message: The Date of Initial Child Protection Conference cannot be a weekend

"""
from typing import Mapping

import pandas as pd

from cin_validator.rule_engine import (
    CINTable,
    IssueLocator,
    RuleContext,
    rule_definition,
)
from cin_validator.test_engine import run_rule

# Get tables and columns of interest from the CINTable object defined in rule_engine/__api.py

Section47 = CINTable.Section47
DateOfInitialCPC = Section47.DateOfInitialCPC


# define characteristics of rule
@rule_definition(
    code="8875",
    module=CINTable.Section47,
    message="The Date of Initial Child Protection Conference cannot be a weekend",
    affected_fields=[DateOfInitialCPC],
)
def validate(
    data_container: Mapping[CINTable, pd.DataFrame], rule_context: RuleContext
):
    df = data_container[Section47]
    # <DateOfInitialCPC> should not be a Saturday, Sunday
    # .weekday() returns the integer value for each day (0-6) with weekends being 5 and 6
    failing_indices = df[df[DateOfInitialCPC].dt.weekday >= 5].index

    rule_context.push_issue(
        table=Section47, field=DateOfInitialCPC, row=failing_indices
    )


def test_validate():
    # Create some sample data such that some values pass the validation and some fail.
    child_identifiers = pd.DataFrame(
        [
            {"DateOfInitialCPC": "17/06/2021"},  # Pass
            {"DateOfInitialCPC": "09/10/2022"},  # Fail, Sunday
            {"DateOfInitialCPC": "14/03/2020"},  # Fail, Saturday
        ],
    )  # 9/10 is a Sunday and 14/3 is a Saturday

    child_identifiers[DateOfInitialCPC] = pd.to_datetime(
        child_identifiers[DateOfInitialCPC], format="%d/%m/%Y", errors="coerce"
    )

    # Run rule function passing in our sample data
    result = run_rule(validate, {Section47: child_identifiers})

    # The result contains a list of issues encountered
    issues = list(result.issues)
    # replace 2 with the number of failing points you expect from the sample data.
    assert len(issues) == 2
    # replace the table and column name as done earlier.
    # The last numbers represent the index values where you expect the sample data to fail the validation check.
    assert issues == [
        IssueLocator(CINTable.Section47, DateOfInitialCPC, 1),
        IssueLocator(CINTable.Section47, DateOfInitialCPC, 2),
    ]

    # Check that the rule definition is what you wrote in the context above.

    assert result.definition.code == "8875"
    assert (
        result.definition.message
        == "The Date of Initial Child Protection Conference cannot be a weekend"
    )
