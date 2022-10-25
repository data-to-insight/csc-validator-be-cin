from typing import Mapping

import pandas as pd

from cin_validator.rule_engine import (
    CINTable,
    IssueLocator,
    RuleContext,
    rule_definition,
)
from cin_validator.test_engine import run_rule
from cin_validator.utils import make_census_period

# Get tables and columns of interest from the CINTable object defined in rule_engine/__api.py
# Replace ChildIdentifiers with the table name, and LAChildID with the column name you want.

ChildIdentifiers = CINTable.ChildIdentifiers
Header = CINTable.Header
PersonBirthDate = ChildIdentifiers.PersonBirthDate
ReferenceDate = Header.ReferenceDate

# define characteristics of rule
@rule_definition(
    code=8520,
    module=CINTable.ChildIdentifiers,
    message="Date of Birth is after data collection period (must be on or before the end of the census period)",
    affected_fields=[PersonBirthDate, ReferenceDate],
)
def validate(
    data_container: Mapping[CINTable, pd.DataFrame], rule_context: RuleContext
):
    # Replace ChildIdentifiers with the name of the table you need.
    df = data_container[ChildIdentifiers]
    df_ref = data_container[Header]

    ref_data_series = df_ref[ReferenceDate]
    collection_start, collection_end = make_census_period(ref_data_series)

    # implement rule logic as described by the Github issue. Put the description as a comment above the implementation as shown.

    # <PersonBirthDate> (N00066) must be on or before <ReferenceDate> (N00603) or null

    df = df[(df[PersonBirthDate] > collection_end)] 

    failing_indices = df.index

    # Replace ChildIdentifiers and LAchildID with the table and column name concerned in your rule, respectively.
    # If there are multiple columns or table, make this sentence multiple times.
    rule_context.push_issue(
        table=ChildIdentifiers, field=PersonBirthDate, row=failing_indices
    )


def test_validate():
    # Create some sample data such that some values pass the validation and some fail.
    
    test_dobs = pd.to_datetime(
        [ 
            "01/06/2021", # 0 pass
            "01/06/2022", # 1 fail
            "31/08/2021", # 2 pass
            "31/12/2022", # 3 fail
            pd.NA, #4 ignored
        ],
        format='%d/%m/%Y',
        errors="coerce"
    )

    fake_dobs = pd.DataFrame({PersonBirthDate: test_dobs})
    fake_header = pd.DataFrame({ReferenceDate: ["31/03/2022"]})

    # Run rule function passing in our sample data
    result = run_rule(validate, {ChildIdentifiers: fake_dobs, Header: fake_header})

    # The result contains a list of issues encountered
    issues = list(result.issues)
    # replace 2 with the number of failing points you expect from the sample data.
    assert len(issues) == 2
    # replace the table and column name as done earlier.
    # The last numbers represent the index values where you expect the sample data to fail the validation check.
    assert issues == [
        IssueLocator(CINTable.ChildIdentifiers, PersonBirthDate, 1),
        IssueLocator(CINTable.ChildIdentifiers, PersonBirthDate, 3),
    ]

    # Check that the rule definition is what you wrote in the context above.

    assert result.definition.code == 8520
    assert result.definition.message == "Date of Birth is after data collection period (must be on or before the end of the census period)"
