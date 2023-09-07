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

ChildProtectionPlans = CINTable.ChildProtectionPlans
NumberOfPreviousCPP = ChildProtectionPlans.NumberOfPreviousCPP


# define characteristics of rule
@rule_definition(
    code="8730",
    module=CINTable.ChildProtectionPlans,
    message="Total Number of previous Child Protection Plans missing",
    affected_fields=[NumberOfPreviousCPP],
)
def validate(
    data_container: Mapping[CINTable, pd.DataFrame], rule_context: RuleContext
):
    # Replace ChildProtectionPlans with the name of the table you need.
    df = data_container[ChildProtectionPlans]

    # implement rule logic as described by the Github issue. Put the description as a comment above the implementation as shown.

    # Where a CPP module is present, <NumberOfPreviousCPP> (N00106) must be greater than or equal to zero
    # Change the line below to ensure values are >=0 ie not null

    failing_indices = df[
        (df[NumberOfPreviousCPP].isna()) | (df[NumberOfPreviousCPP].astype("Int64") < 0)
    ].index
    # Int64 dtype is used instead of int because it tolerates the possibility of NaN values in the column
    # nullable integers ==  Int64 dtype

    rule_context.push_issue(
        table=ChildProtectionPlans, field=NumberOfPreviousCPP, row=failing_indices
    )


def test_validate():
    # Create some sample data such that some values pass the validation and some fail.
    fake_data = pd.DataFrame(
        [[1234], [pd.NA], [pd.NA], [-1]], columns=[NumberOfPreviousCPP]
    )

    # Run rule function passing in our sample data
    result = run_rule(validate, {ChildProtectionPlans: fake_data})
    # The result contains a list of issues encountered
    issues = list(result.issues)
    # replace 3 with the number of failing points you expect from the sample data.
    assert len(issues) == 3
    # replace the table and column name as done earlier.
    # The last numbers represent the index values where you expect the sample data to fail the validation check.
    assert issues == [
        IssueLocator(CINTable.ChildProtectionPlans, NumberOfPreviousCPP, 1),
        IssueLocator(CINTable.ChildProtectionPlans, NumberOfPreviousCPP, 2),
        IssueLocator(CINTable.ChildProtectionPlans, NumberOfPreviousCPP, 3),
    ]

    # Check that the rule definition is what you wrote in the context above.

    # replace '8730' with the rule code and put the appropriate message in its place too.
    assert result.definition.code == "8730"
    assert (
        result.definition.message
        == "Total Number of previous Child Protection Plans missing"
    )
