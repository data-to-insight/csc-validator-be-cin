from sqlite3 import Date
from typing import Mapping

import pandas as pd

from cin_validator.rule_engine import rule_definition, CINTable, RuleContext
from cin_validator.rule_engine import IssueLocator
from cin_validator.rules.cin2022_23.rule_8500 import ChildIdentifiers, LAchildID
from cin_validator.test_engine import run_rule

# Get tables and columns of interest from the CINTable object defined in rule_engine/__api.py
# Replace ChildIdentifiers with the table name, and LAChildID with the column name you want.

Section47 = CINTable.Section47
DateOfInitialCPC = Section47.DateOfInitialCPC

# define characteristics of rule
@rule_definition(
    code=8715,
    module=CINTable.Section47,
    message="Date of Initial Child Protection Conference must fall within the census year",
    affected_fields=[DateOfInitialCPC],
    # Do I also include ReferenceDate from Header table as <ReferenceDate>?
    
)
def validate(
    data_container: Mapping[CINTable, pd.DataFrame], rule_context: RuleContext
):
    # Replace ChildIdentifiers with the name of the table you need.
    df = data_container[Section47]
    print (df)

    # implement rule logic as described by the Github issue. Put the description as a comment above the implementation as shown.

    # Date of Initial Child Protection Conference must fall within the census year
    failing_indices = df[df[DateOfInitialCPC].isna()].index
    collection_start = pd.to_datetime('01/04/2021', format= '%d/%m/%Y')
    collection_end = pd.to_datetime('31/03/2022', format= '%d/%m/%Y')
    
    print (collection_end)
    print (collection_start)

    failing_indices = ((pd.to_datetime(DateOfInitialCPC, format = '%d/%m/%Y') >= collection_start) & (pd.to_datetime(DateOfInitialCPC, format= '%d/%m/%Y') <= collection_end)).index
    print (failing_indices)

    # Replace ChildIdentifiers and LAchildID with the table and column name concerned in your rule, respectively.
    # If there are multiple columns or table, make this sentence multiple times.
    rule_context.push_issue(
        table=Section47, field=DateOfInitialCPC, row=failing_indices
        
            )

def test_validate():
    # Create some sample data such that some values pass the validation and some fail.
    child_identifiers = pd.DataFrame([['01/03/2019'], ['01/04/2021'], ['01/10/2022']], columns=[DateOfInitialCPC])
    
    # Run rule function passing in our sample data
    result = run_rule(validate, {Section47: child_identifiers})

    # The result contains a list of issues encountered
    issues = list(result.issues)
    # replace 2 with the number of failing points you expect from the sample data.
    assert len(issues) == 2
    # replace the table and column name as done earlier.
    # The last numbers represent the index values where you expect the sample data to fail the validation check.
    assert issues == [
        IssueLocator(CINTable.Section47, DateOfInitialCPC, 0),
        IssueLocator(CINTable.Section47, DateOfInitialCPC, 1),
        
    ]

    # Check that the rule definition is what you wrote in the context above.

    # replace 8500 with the rule code and put the appropriate message in its place too.
    assert result.definition.code == 8715
    assert result.definition.message == "Date of Initial Child Protection Conference must fall within the census year"