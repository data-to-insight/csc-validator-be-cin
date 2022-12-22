"""
Rule number: 1520
Module: Child idenitifiers
Rule details: Each pupil <UPN> (N00001) must be unique across all pupils in the extract. 
Note: This rule should be evaluated at LA-level for imported data                                                                     
Rule message: More than one record with the same UPN
"""
from typing import Mapping

import pandas as pd

from cin_validator.rule_engine import CINTable, RuleContext, rule_definition
from cin_validator.test_engine import run_rule

# Get tables and columns of interest from the CINTable object defined in rule_engine/__api.py

ChildIdentifiers = CINTable.ChildIdentifiers
UPN = ChildIdentifiers.UPN

# define characteristics of rule
@rule_definition(
    code=1520,
    module=CINTable.ChildIdentifiers,
    message="More than one record with the same UPN",
    affected_fields=[UPN],
)
def validate(
    data_container: Mapping[CINTable, pd.DataFrame], rule_context: RuleContext
):
    df = data_container[ChildIdentifiers]

    # Each pupil <UPN> (N00001) must be unique across all pupils in the extract

    failing_indices = df[df.duplicated(subset=[UPN], keep=False)].index

    if len(failing_indices) > 0:
        rule_context.push_la_level(
            rule_context.definition.code, rule_context.definition.message
        )
    else:
        pass


def test_validate():
    # Create some sample data such that some values pass the validation and some fail.
    child_identifiers = pd.DataFrame([[1234], [1234], [346546]], columns=[UPN])

    # Run rule function passing in our sample data
    result = run_rule(validate, {ChildIdentifiers: child_identifiers})

    issues = result.la_issues
    assert issues == (1520, "More than one record with the same UPN")
