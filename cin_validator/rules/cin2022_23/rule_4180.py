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
# Replace ChildIdentifiers with the table name, and GenderCurrent with the column name you want.

ChildIdentifiers = CINTable.ChildIdentifiers
GenderCurrent = ChildIdentifiers.GenderCurrent


# define characteristics of rule
@rule_definition(
    # write the rule code here, in place of 8500
    code="4180",
    # replace ChildIdentifiers with the value in the module column of the excel sheet corresponding to this rule .
    module=CINTable.ChildIdentifiers,
    # replace the message with the corresponding value for this rule, gotten from the excel sheet.
    message="Gender is missing",
    # The column names tend to be the words within the < > signs in the github issue description.
    affected_fields=[GenderCurrent],
)
def validate(
    data_container: Mapping[CINTable, pd.DataFrame], rule_context: RuleContext
):
    # Replace ChildIdentifiers with the name of the table you need.
    df = data_container[ChildIdentifiers]

    # implement rule logic as described by the Github issue. Put the description as a comment above the implementation as shown.

    valid_gender_codes = ["1", "2", "0", "9"]

    # <GenderCurrent> (N00097) must be present and valid

    failing_indices = df[
        df[GenderCurrent].isna()
        | (~df[GenderCurrent].astype("str").isin(valid_gender_codes))
    ].index

    # Replace ChildIdentifiers and GenderCurrent with the table and column name concerned in your rule, respectively.
    # If there are multiple columns or table, make this sentence multiple times.
    rule_context.push_issue(
        table=ChildIdentifiers, field=GenderCurrent, row=failing_indices
    )


def test_validate():
    # Create some sample data such that some values pass the validation and some fail.
    child_identifiers = pd.DataFrame(
        [1, pd.NA, 7, "Male", 2, 0, 9, "9"], columns=[GenderCurrent]
    )

    # Run rule function passing in our sample data
    result = run_rule(validate, {ChildIdentifiers: child_identifiers})

    # The result contains a list of issues encountered
    issues = list(result.issues)
    # replace 3 with the number of failing points you expect from the sample data.
    assert len(issues) == 3
    # replace the table and column name as done earlier.
    # The last numbers represent the index values where you expect the sample data to fail the validation check.
    assert issues == [
        IssueLocator(CINTable.ChildIdentifiers, GenderCurrent, 1),
        IssueLocator(CINTable.ChildIdentifiers, GenderCurrent, 2),
        IssueLocator(CINTable.ChildIdentifiers, GenderCurrent, 3),
    ]

    # Check that the rule definition is what you wrote in the context above.

    # replace '4180' with the rule code and put the appropriate message in its place too.
    assert result.definition.code == "4180"
    assert result.definition.message == "Gender is missing"
