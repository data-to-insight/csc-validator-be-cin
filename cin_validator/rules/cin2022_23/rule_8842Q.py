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
# Replace ChildIdentifiers with the table name, and LAChildID with the column name you want.

Reviews = CINTable.Reviews
CPPreviewDate = Reviews.CPPreviewDate

# define characteristics of rule
@rule_definition(
    # write the rule code here, in place of 8500
    code="8842Q",
    # replace ChildIdentifiers with the value in the module column of the excel sheet corresponding to this rule .
    module=CINTable.Reviews,
    rule_type=RuleType.QUERY,
    # replace the message with the corresponding value for this rule, gotten from the excel sheet.
    message="Please check: Review Record has a missing date",
    # The column names tend to be the words within the < > signs in the github issue description.
    affected_fields=[CPPreviewDate],
)
def validate(
    data_container: Mapping[CINTable, pd.DataFrame], rule_context: RuleContext
):
    # Replace ChildIdentifiers with the name of the table you need.
    df = data_container[Reviews]

    # implement rule logic as described by the Github issue. Put the description as a comment above the implementation as shown.

    # Where a <Reviews> group is present, a valid <CPPreviewdate> (N00116) should be present within the group
    # Valid values for columns can be found in this document:
    # https://assets.publishing.service.gov.uk/government/uploads/system/uploads/attachment_data/file/1025195/Children_in_need_census_2022_to_2023_guide.pdf
    condition = df[CPPreviewDate].isna()

    failing_indices = df[condition].index

    # Replace ChildIdentifiers and LAchildID with the table and column name concerned in your rule, respectively.
    # If there are multiple columns or table, make this sentence multiple times.
    rule_context.push_issue(table=Reviews, field=CPPreviewDate, row=failing_indices)


def test_validate():
    # Create some sample data such that some values pass the validation and some fail.
    reviews = pd.DataFrame([[1234], [pd.NA], [pd.NA]], columns=[CPPreviewDate])

    # Run rule function passing in our sample data
    result = run_rule(validate, {Reviews: reviews})

    # The result contains a list of issues encountered
    issues = list(result.issues)
    # replace 2 with the number of failing points you expect from the sample data.
    assert len(issues) == 2
    # replace the table and column name as done earlier.
    # The last numbers represent the index values where you expect the sample data to fail the validation check.
    assert issues == [
        IssueLocator(CINTable.Reviews, CPPreviewDate, 1),
        IssueLocator(CINTable.Reviews, CPPreviewDate, 2),
    ]

    # Check that the rule definition is what you wrote in the context above.

    # replace 8500 with the rule code and put the appropriate message in its place too.
    assert result.definition.code == "8842Q"
    assert result.definition.message == "Please check: Review Record has a missing date"
