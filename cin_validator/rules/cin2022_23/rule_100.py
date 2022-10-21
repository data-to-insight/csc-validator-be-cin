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
# Replace ChildIdentifiers with the table name, and LAChildID with the column name you want.

Header = CINTable.Header
ReferenceDate = Header.ReferenceDate

# define characteristics of rule
@rule_definition(
    # write the rule code here, in place of 100
    code=100,
    # replace Header with the value in the module column of the excel sheet corresponding to this rule .
    module=CINTable.Header,
    # replace the message with the corresponding value for this rule, gotten from the excel sheet.
    message="Reference Date is incorrect",
    # The column names tend to be the words within the < > signs in the github issue description.
    affected_fields=[ReferenceDate],
)
def validate(
    data_container: Mapping[CINTable, pd.DataFrame], rule_context: RuleContext
):
    # Replace ChildIdentifiers with the name of the table you need.
    df = data_container[Header]

    # implement rule logic as descriped by the Github issue. Put the description as a comment above the implementation as shown.

    # <ReferenceDate> (N00603) must be present and must equal 2022-03-31
    df[ReferenceDate] = pd.to_datetime(
        df[ReferenceDate], format="%d/%m/%Y", errors="coerce"
    )
    # Checks that the reference date is present
    is_present = df[ReferenceDate].isna()
    # Checks the error date is equal to 31/03/2022.
    error_date = df[ReferenceDate] != pd.to_datetime(
        "31/03/2022", format="%d/%m/%Y", errors="coerce"
    )
    failing_indices = df[is_present | error_date].index

    # Replace Header and ReferenceDate with the table and column name concerned in your rule, respectively.
    # If there are multiple columns or table, make this sentence multiple times.
    rule_context.push_issue(table=Header, field=ReferenceDate, row=failing_indices)


def test_validate():
    # Create some sample data such that some values pass the validation and some fail.
    header = pd.DataFrame(
        [["31/03/2022"], [pd.NA], [pd.NA], ["30/11/2021"]], columns=[ReferenceDate]
    )

    # Run rule function passing in our sample data
    result = run_rule(validate, {Header: header})

    # The result contains a list of issues encountered
    issues = list(result.issues)
    # replace 3 with the number of failing points you expect from the sample data.
    assert len(issues) == 3
    # replace the table and column name as done earlier.
    # The last numbers represent the index values where you expect the sample data to fail the validation check.
    assert issues == [
        IssueLocator(CINTable.Header, ReferenceDate, 1),
        IssueLocator(CINTable.Header, ReferenceDate, 2),
        IssueLocator(CINTable.Header, ReferenceDate, 3),
    ]

    # Check that the rule definition is what you wrote in the context above.

    # replace 100 with the rule code and put the appropriate message in its place too.
    assert result.definition.code == 100
    assert result.definition.message == "Reference Date is incorrect"
