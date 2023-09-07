from typing import Mapping

import pandas as pd

from cin_validator.rule_engine import (
    CINTable,
    IssueLocator,
    RuleContext,
    rule_definition,
)
from cin_validator.test_engine import run_rule

CINdetails = CINTable.CINdetails
ReasonForClosure = CINdetails.ReasonForClosure


# define characteristics of rule
@rule_definition(
    code="8640",
    module=CINTable.CINdetails,
    message="CIN Reason for closure code invalid (see Reason for Closure table in CIN Census code set)",
    affected_fields=[ReasonForClosure],
)
def validate(
    data_container: Mapping[CINTable, pd.DataFrame], rule_context: RuleContext
):
    df = data_container[CINdetails]

    # implement rule logic as described by the Github issue. Put the description as a comment above the implementation as shown.

    # Reason for Closure must be a valid reason for closure code value as shown in the list below

    valid_reason = ["RC1", "RC2", "RC3", "RC4", "RC5", "RC6", "RC7", "RC8", "RC9"]

    # Check if the Reason For Closure is not in the list of valid reasons and a value has been entered.

    df = df[
        ~(df["ReasonForClosure"].isin(valid_reason)) & df["ReasonForClosure"].notna()
    ]

    failing_indices = df.index

    # Replace CINdetails and ReasonForClosure with the table and column name concerned in your rule, respectively.
    # If there are multiple columns or table, make this sentence multiple times.
    rule_context.push_issue(
        table=CINdetails, field=ReasonForClosure, row=failing_indices
    )


def test_validate():
    # Sample test all will pass the validation with the exception RC13 and RC0 which are not valid Closure codes.
    fake_data = ["RC1", "RC13", pd.NA, "RC0"]

    fake_dataframe = pd.DataFrame({"ReasonForClosure": fake_data})

    # Run rule function passing in our sample test data
    result = run_rule(validate, {CINdetails: fake_dataframe})

    # The result contains a list of issues encountered
    issues = list(result.issues)
    # replace 2 with the number of failing points you expect from the sample data.
    assert len(issues) == 2
    # replace the table and column name as done earlier.
    # The last numbers represent the index values where you expect the sample data to fail the validation check.
    assert issues == [
        IssueLocator(CINTable.CINdetails, ReasonForClosure, 1),
        IssueLocator(CINTable.CINdetails, ReasonForClosure, 3),
    ]

    # Check that the rule definition is what you wrote in the context above.

    assert result.definition.code == "8640"
    assert (
        result.definition.message
        == "CIN Reason for closure code invalid (see Reason for Closure table in CIN Census code set)"
    )
