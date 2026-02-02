from typing import Mapping

import pandas as pd

from cin_validator.rule_engine import CINTable, RuleContext, RuleType, rule_definition
from cin_validator.test_engine import run_rule

PreProceedings = CINTable.PreProceedings


# define characteristics of rule
@rule_definition(
    code="8995Q",
    rule_type=RuleType.QUERY,
    module=CINTable.PreProceedings,
    message="Please check and provide a reason why no module 5 (pre-proceedings and FGM) data has been provided for any child.",
    affected_fields=[],
)
def validate(
    data_container: Mapping[CINTable, pd.DataFrame], rule_context: RuleContext
):
    df = data_container[PreProceedings]

    if len(df) < 1:
        rule_context.push_la_level(
            rule_context.definition.code, rule_context.definition.message
        )
    else:
        pass


def test_validate():
    sample_pre_proceedings = pd.DataFrame()

    # Run rule function passing in our sample data
    result = run_rule(validate, {PreProceedings: sample_pre_proceedings})

    assert result.la_issues == (
        "8995Q",
        "Please check and provide a reason why no module 5 (pre-proceedings and FGM) data has been provided for any child.",
    )
