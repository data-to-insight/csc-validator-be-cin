from typing import Mapping

import pandas as pd

from cin_validator.rule_engine import rule_definition, CINTable, RuleContext
from cin_validator.rule_engine import IssueLocator
from cin_validator.test_engine import run_rule

# Get tables and columns of interest from the CINTable object defined in rule_engine/__api.py
# Replace ChildIdentifiers with the table name, and LAChildID with the column name you want.

ChildIdentifiers = CINTable.ChildIdentifiers
UPN = ChildIdentifiers.UPN

# define characteristics of rule
@rule_definition(
    # write the rule code here, in place of 8500
    code=1550,
    # replace ChildIdentifiers with the value in the module column of the excel sheet corresponding to this rule .
    module=CINTable.ChildIdentifiers,
    # replace the message with the corresponding value for this rule, gotten from the excel sheet.
    message="UPN invalid (character 13 not a recognised value)",
    # The column names tend to be the words within the < > signs in the github issue description.
    affected_fields=[UPN],
)
def validate(
    data_container: Mapping[CINTable, pd.DataFrame], rule_context: RuleContext
):
    # Replace ChildIdentifiers with the name of the table you need.
    df = data_container[ChildIdentifiers]

    # if <UPN> (N00001)) present Character 13 of <UPN> must be numeric or A-Z omitting I, O and S
    # Confirm length and value present
    df2 = df[(df["UPN"].str.len() == 13) & df["UPN"].notna()]

    # Valid characters
    valid = ['A','B','C','D','E','F','G','H','J','K','L','M','N','P','Q','R','T','U','V','W','Y','X','Z','0','1','2','3','4','5','6','7','8','9']

    failing_indices = df2[~df2["UPN"].str[12].isin(valid)].index

    # Replace ChildIdentifiers and LAchildID with the table and column name concerned in your rule, respectively. 
    # If there are multiple columns or table, make this sentence multiple times.
    rule_context.push_issue(
        table=ChildIdentifiers, field=UPN, row=failing_indices
    )


def test_validate():
    # Create some sample data such that some values pass the validation and some fail.
    child_identifiers = pd.DataFrame([['1234567891234'], ['123456789123I'], ['123456789123O']], columns=[UPN])

    # Run rule function passing in our sample data
    result = run_rule(validate, {ChildIdentifiers: child_identifiers})

    # The result contains a list of issues encountered
    issues = list(result.issues)
    # replace 2 with the number of failing points you expect from the sample data.
    assert len(issues) == 2
    # replace the table and column name as done earlier. 
    # The last numbers represent the index values where you expect the sample data to fail the validation check.
    assert issues == [
        IssueLocator(CINTable.ChildIdentifiers, UPN, 1),
        IssueLocator(CINTable.ChildIdentifiers, UPN, 2),
    ]

    # Check that the rule definition is what you wrote in the context above.

    # replace 8500 with the rule code and put the appropriate message in its place too.
    assert result.definition.code == 1550
    assert result.definition.message == "UPN invalid (character 13 not a recognised value)"
