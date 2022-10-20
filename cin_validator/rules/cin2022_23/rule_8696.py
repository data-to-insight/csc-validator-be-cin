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

Assessments = CINTable.Assessments
AssessmentAuthorisationDate = Assessments.AssessmentAuthorisationDate

# define characteristics of rule
@rule_definition(
    code=8696,
    module=CINTable.Assessments,
    message="Assessment end date must fall within the census year",
    affected_fields=[AssessmentAuthorisationDate],
    # Do I also include ReferenceDate from Header table as <ReferenceDate>?
)    
def validate(
    data_container: Mapping[CINTable, pd.DataFrame], rule_context: RuleContext
):
    # Replace ChildIdentifiers with the name of the table you need.
    df = data_container[Assessments]
   

    # implement rule logic as described by the Github issue. Put the description as a comment above the implementation as shown.

    # <If present <AssessmentAuthorisationDate> (N00160) must be on or between [Start_Of_Census_Year] and <ReferenceDate> (N00603)
    failing_indices = df[df[AssessmentAuthorisationDate].isna()].index
    
    collection_start = pd.to_datetime('01/04/2021', format= '%d/%m/%Y')
    collection_end = pd.to_datetime('31/03/2022', format= '%d/%m/%Y')

    failing_indices = ((pd.to_datetime(AssessmentAuthorisationDate, format = '%d/%m/%Y') >= collection_start) & (pd.to_datetime(AssessmentAuthorisationDate, format= '%d/%m/%Y') <= collection_end)).index

    # Replace ChildIdentifiers and LAchildID with the table and column name concerned in your rule, respectively.
    # If there are multiple columns or table, make this sentence multiple times.
    rule_context.push_issue(
        table=Assessments, field=AssessmentAuthorisationDate, row=failing_indices
    )


def test_validate():
    # Create some sample data such that some values pass the validation and some fail.
    child_identifiers = pd.DataFrame([['01/03/2019'], [pd.NA], ['01/10/2022']], columns=[AssessmentAuthorisationDate])

    # Run rule function passing in our sample data
    result = run_rule(validate, {Assessments: child_identifiers})

    # The result contains a list of issues encountered
    issues = list(result.issues)
    # replace 2 with the number of failing points you expect from the sample data.
    assert len(issues) == 2
    # replace the table and column name as done earlier.
    # The last numbers represent the index values where you expect the sample data to fail the validation check.
    assert issues == [
        IssueLocator(CINTable.Assessments, AssessmentAuthorisationDate, 0),
        IssueLocator(CINTable.Assessments, AssessmentAuthorisationDate, 1),
    ]

    # Check that the rule definition is what you wrote in the context above.

    assert result.definition.code == 8696
    assert result.definition.message == "Assessment end date must fall within the census year"
