from typing import Mapping

import pandas as pd

from cin_validator.rule_engine import rule_definition, CINTable, RuleContext
from cin_validator.rule_engine import IssueLocator
from cin_validator.test_engine import run_rule

# Get tables and columns of interest from the CINTable object defined in rule_engine/__api.py
# Replace ChildIdentifiers with the table name, and LAChildID with the column name you want.

ChildProtectionPlans = CINTable.ChildProtectionPlans
NumberOfPreviousCPP = ChildProtectionPlans.NumberOfPreviousCPP

# define characteristics of rule
@rule_definition(
    # write the rule code here, in place of 8500
    code=8730,
    # replace ChildIdentifiers with the value in the module column of the excel sheet corresponding to this rule .
    module=CINTable.ChildProtectionPlans,
    # replace the message with the corresponding value for this rule, gotten from the excel sheet.
    message="Total Number of previous Child Protection Plans missing",
    # The column names tend to be the words within the < > signs in the github issue description.
    affected_fields=[NumberOfPreviousCPP],
)
def validate(
    data_container: Mapping[CINTable, pd.DataFrame], rule_context: RuleContext
):
    # Replace ChildIdentifiers with the name of the table you need.
    df = data_container[ChildProtectionPlans]

    # implement rule logic as described by the Github issue. Put the description as a comment above the implementation as shown.

    # Where a CPP module is present, <NumberOfPreviousCPP> (N00106) must be greater than or equal to zero
    # Change the line below to ensure values are >=0 ie not null

    # <LAchildID> (N00097) must be present
    failing_indices = df[
        (df[NumberOfPreviousCPP].isna()) | (df[NumberOfPreviousCPP] < 0)
    ].index

    # Replace ChildIdentifiers and LAchildID with the table and column name concerned in your rule, respectively.
    # If there are multiple columns or table, make this sentence multiple times.
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
    # replace 2 with the number of failing points you expect from the sample data.
    assert len(issues) == 3
    # replace the table and column name as done earlier.
    # The last numbers represent the index values where you expect the sample data to fail the validation check.
    assert issues == [
        IssueLocator(CINTable.ChildProtectionPlans, NumberOfPreviousCPP, 1),
        IssueLocator(CINTable.ChildProtectionPlans, NumberOfPreviousCPP, 2),
        IssueLocator(CINTable.ChildProtectionPlans, NumberOfPreviousCPP, 3),
    ]

    # Check that the rule definition is what you wrote in the context above.

    # replace 8500 with the rule code and put the appropriate message in its place too.
    assert result.definition.code == 8730
    assert (
        result.definition.message
        == "Total Number of previous Child Protection Plans missing"
    )
