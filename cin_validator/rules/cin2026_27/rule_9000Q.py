from typing import Mapping

import pandas as pd

from cin_validator.rule_engine import CINTable, RuleContext, RuleType, rule_definition
from cin_validator.test_engine import run_rule

PreProceedings = CINTable.PreProceedings
PPStartDate = PreProceedings.PPStartDate


# define characteristics of rule
@rule_definition(
    code="9000Q",
    rule_type=RuleType.QUERY,
    module=CINTable.PreProceedings,
    message="Please check: If you do not yet collect this data, please leave this field blank and provide a reason.",
    affected_fields=[],
)
def validate(
    data_container: Mapping[CINTable, pd.DataFrame], rule_context: RuleContext
):
    # (N00826) has not been provided
    # It's unclear from the provided info if this checks per child's Pre Proceeding block, or for the entire LA. I have left it as entire LA for now.
    df = data_container[PreProceedings]

    start_dates = len(df["PPStartDate"].notna())

    if start_dates < 1:
        rule_context.push_la_level(
            rule_context.definition.code, rule_context.definition.message
        )
    else:
        pass


def test_validate():
    sample_pre_proceedings = pd.DataFrame({"PPStartDate": []})

    # Run rule function passing in our sample data
    result = run_rule(validate, {PreProceedings: sample_pre_proceedings})

    assert result.la_issues == (
        "9000Q",
        "Please check: If you do not yet collect this data, please leave this field blank and provide a reason.",
    )
