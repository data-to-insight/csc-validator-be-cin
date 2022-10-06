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
    code=1540,
    # replace ChildIdentifiers with the value in the module column of the excel sheet corresponding to this rule .
    module=CINTable.ChildIdentifiers,
    # replace the message with the corresponding value for this rule, gotten from the excel sheet.
    message="UPN invalid (characters 5-12 not all numeric)",
    # The column names tend to be the words within the < > signs in the github issue description.
    affected_fields=[UPN],
)
def validate(
    data_container: Mapping[CINTable, pd.DataFrame], rule_context: RuleContext
):
    # Replace ChildIdentifiers with the name of the table you need.
    df = data_container[ChildIdentifiers]

    # implement rule logic as descriped by the Github issue. Put the description as a comment above the implementation as shown.

    # If <UPN> (N00001) present Characters 5-12 of <UPN> must be numeric

    #  df takes a slice of rows of df where the UPN column doesn't have Na/NaN values
    df = df.loc[df["UPN"].notna()]
    """
    Returns indices of rows where character 5:12 of UPN contains non numerical characters.
    Does this by:
    Returning a boolean for the logic check to see is characters 5:12 contain only numerical characters.
    Using the not operator (~) to return values as false where the logic returns true (and true if there are non-numeric characters).
    Slicing df according to this criteria. 
    Returns indices of the rows of this df to failing_indices."""
    failing_indices = df[~df["UPN"].str[4:12].str.isdigit()].index

    # Replace ChildIdentifiers and LAchildID with the table and column name concerned in your rule, respectively.
    # If there are multiple columns or table, make this sentence multiple times.
    rule_context.push_issue(table=ChildIdentifiers, field=UPN, row=failing_indices)


def test_validate():
    # Create some sample data such that some values pass the validation and some fail.
    child_identifiers = pd.DataFrame(
        {"UPN": [pd.NA, "X000000000000", "X0000y0000000", "x0000000er00e0"]}
    )

    # Run rule function passing in our sample data
    result = run_rule(validate, {ChildIdentifiers: child_identifiers})

    # The result contains a list of issues encountered
    issues = list(result.issues)
    # replace 2 with the number of failing points you expect from the sample data.
    assert len(issues) == 2
    # replace the table and column name as done earlier.
    # The last numbers represent the index values where you expect the sample data to fail the validation check.
    assert issues == [
        IssueLocator(CINTable.ChildIdentifiers, UPN, 2),
        IssueLocator(CINTable.ChildIdentifiers, UPN, 3),
    ]

    # Check that the rule definition is what you wrote in the context above.

    # replace 8500 with the rule code and put the appropriate message in its place too.
    assert result.definition.code == 1540
    assert result.definition.message == "UPN invalid (characters 5-12 not all numeric)"
