"""
Rule number: 8870Q
Module: Section 47
Rule details: Where present, the <InitialCPCtarget> (N00109) should not be a Saturday, Sunday

Rule message: Please check: The Target Date for Initial Child Protection Conference should not be a weekend

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

# Get tables and columns of interest from the CINTable object defined in rule_engine/__api.py

Section47 = CINTable.Section47
InitialCPCtarget = Section47.InitialCPCtarget


# define characteristics of rule
@rule_definition(
    code="8870Q",
    module=CINTable.Section47,
    rule_type=RuleType.QUERY,
    message="Please check and either amend or provide a reason: The Target Date for Initial Child Protection Conference should not be a weekend",
    affected_fields=[InitialCPCtarget],
)
def validate(
    data_container: Mapping[CINTable, pd.DataFrame], rule_context: RuleContext
):
    df = data_container[Section47]

    # <InitialCPCtarget> (N00109) should not be a Saturday, Sunday
    # Convert column to date format
    df[InitialCPCtarget] = pd.to_datetime(
        df[InitialCPCtarget], format="%d-%m-%Y", errors="coerce"
    )

    # .weekday() returns the integer value for each day (0-6) with weekends being 5 and 6
    failing_indices = df[df[InitialCPCtarget].dt.weekday >= 5].index

    rule_context.push_issue(
        table=Section47, field=InitialCPCtarget, row=failing_indices
    )


def test_validate():
    # Create some sample data such that some values pass the validation and some fail.
    child_identifiers = pd.DataFrame(
        [["17-06-2021"], ["09-10-2022"], ["14-03-2020"]], columns=[InitialCPCtarget]
    )  # 9/10 is a Sunday and 14/3 is a Saturday

    # Run rule function passing in our sample data
    result = run_rule(validate, {Section47: child_identifiers})

    # The result contains a list of issues encountered
    issues = list(result.issues)
    # replace 2 with the number of failing points you expect from the sample data.
    assert len(issues) == 2
    # replace the table and column name as done earlier.
    # The last numbers represent the index values where you expect the sample data to fail the validation check.
    assert issues == [
        IssueLocator(CINTable.Section47, InitialCPCtarget, 1),
        IssueLocator(CINTable.Section47, InitialCPCtarget, 2),
    ]

    # Check that the rule definition is what you wrote in the context above.

    assert result.definition.code == "8870Q"
    assert (
        result.definition.message
        == "Please check and either amend or provide a reason: The Target Date for Initial Child Protection Conference should not be a weekend"
    )
