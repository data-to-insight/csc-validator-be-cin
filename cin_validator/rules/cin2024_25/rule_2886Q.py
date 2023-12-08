from typing import Mapping

import pandas as pd

from cin_validator.rule_engine import CINTable, RuleContext, RuleType, rule_definition
from cin_validator.test_engine import run_rule

ChildIdentifiers = CINTable.ChildIdentifiers
Sex = ChildIdentifiers.Sex
ExpectedPersonBirthDate = ChildIdentifiers.ExpectedPersonBirthDate


# define characteristics of rule
@rule_definition(
    code="2886Q",
    rule_type=RuleType.QUERY,
    module=CINTable.ChildIdentifiers,
    message="Please check and either amend or provide a reason: Percentage of children with no sex recorded is more than 2% (excluding unborns)",
    affected_fields=[ExpectedPersonBirthDate, ChildIdentifiers],
)
def validate(
    data_container: Mapping[CINTable, pd.DataFrame], rule_context: RuleContext
):
    df = data_container[ChildIdentifiers]

    # The sum of: the number of child records where <Sex> (N00065) is equal to zero or missing,
    # and the <ExpectedPersonBirthDate> (N00098) is equal to missing, divided by the total number of
    # child records should be less than or equal to 0.02. Validation should be triggered at LA level.

    # get the total number of child records
    num_records = len(df)

    # get the number of child records that fit the specified condition.
    missing_gender = (df[Sex].isna()) | (df[Sex] == "U")
    missing_date = df[ExpectedPersonBirthDate].isna()
    condition = missing_gender & missing_date
    # since the filtered number has to be compared to the original, make a copy of the data.
    df_issues = df.copy()
    df_issues = df_issues[condition]
    num_issues = len(df_issues)

    # calculate
    missing_ratio = num_issues / num_records
    if missing_ratio > 0.02:
        rule_context.push_la_level(
            rule_context.definition.code, rule_context.definition.message
        )
    else:
        pass


def test_validate():
    sample_child_ids = pd.DataFrame(
        [
            {Sex: 0, ExpectedPersonBirthDate: pd.NA},
            {Sex: 1, ExpectedPersonBirthDate: pd.NA},
            {Sex: pd.NA, ExpectedPersonBirthDate: pd.NA},
            {Sex: "U", ExpectedPersonBirthDate: pd.NA},
        ]
    )

    # Run rule function passing in our sample data
    result = run_rule(validate, {ChildIdentifiers: sample_child_ids})

    assert result.la_issues == (
        "2886Q",
        "Please check and either amend or provide a reason: Percentage of children with no sex recorded is more than 2% (excluding unborns)",
    )
