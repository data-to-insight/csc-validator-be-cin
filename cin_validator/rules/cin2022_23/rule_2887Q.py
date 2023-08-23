from typing import Mapping

import pandas as pd

from cin_validator.rule_engine import CINTable, RuleContext, RuleType, rule_definition
from cin_validator.test_engine import run_rule

Disabilities = CINTable.Disabilities
Disability = Disabilities.Disability
LAchildID = Disabilities.LAchildID


# define characteristics of rule
@rule_definition(
    code="2887Q",
    rule_type=RuleType.QUERY,
    module=CINTable.Disabilities,
    message="Please check and either amend or provide a reason: Less than 8 disability codes have been used in your return",
    affected_fields=[Disability],
)
def validate(
    data_container: Mapping[CINTable, pd.DataFrame], rule_context: RuleContext
):
    df = data_container[Disabilities]

    # LOGIC
    # Excluding children where <Disability> (N00099) is ‘NONE’, the number of different disability codes used by a LA should be more than 7.
    # Validation should be triggered at LA level, not child level, if the LA has only used 7 or fewer different disability codes in their return.

    # remove "NONE" values
    condition = df[Disability] == "NONE"
    df = df[condition]

    # .nunique() will include NaN values so .count() is used instead which excludes NaNs.
    num_disabilities = df[Disability].count()
    if num_disabilities <= 7:
        rule_context.push_la_level(
            rule_context.definition.code, rule_context.definition.message
        )
    else:
        pass


def test_validate():
    sample_disabilities = pd.DataFrame(
        [
            {
                LAchildID: "child1",
                Disability: 0,
            },
            {LAchildID: "child1", Disability: "aaaa"},
            {LAchildID: "child2", Disability: "bbbb"},
            {LAchildID: "child3", Disability: "aaaa"},  # duplicate
            {LAchildID: "child4", Disability: "cc"},
            {LAchildID: "child5", Disability: "d"},
            {LAchildID: "child6", Disability: pd.NA},  # not counted
            {LAchildID: "child7", Disability: "f"},
            {LAchildID: "child8", Disability: "NONE"},  # ignored: Disability is NONE
            {LAchildID: "child9", Disability: "aaaa"},  # duplicate
        ]
    )

    # Run rule function passing in our sample data
    result = run_rule(validate, {Disabilities: sample_disabilities})

    assert result.la_issues == (
        "2887Q",
        "Please check and either amend or provide a reason: Less than 8 disability codes have been used in your return",
    )
