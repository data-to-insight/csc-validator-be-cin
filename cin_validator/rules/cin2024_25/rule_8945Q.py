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

Assessments = CINTable.Assessments
AssessmentFactors = Assessments.AssessmentFactors


# define characteristics of rule
@rule_definition(
    code="8945Q",
    rule_type=RuleType.QUERY,
    module=CINTable.Assessments,
    message="Please check and either amend data or provide a reason: the assessment factors code '18A' should not be used ('18B' or '18C' should be used instead)",
    affected_fields=[AssessmentFactors],
)
def validate(
    data_container: Mapping[CINTable, pd.DataFrame], rule_context: RuleContext
):
    df = data_container[Assessments].copy()

    with_af = df[df[AssessmentFactors].notna()]
    failing_indices = with_af[with_af[AssessmentFactors].str.upper() == "18A"].index

    rule_context.push_issue(
        table=Assessments, field=AssessmentFactors, row=failing_indices
    )


def test_validate():
    sample_assessments = pd.DataFrame(
        {"AssessmentFactors": [pd.NA, "18a", "18A", "18B", "18C"]}
    )

    # Run rule function passing in our sample data
    result = run_rule(validate, {Assessments: sample_assessments})

    issues = list(result.issues)
    # Intended fail points in data.
    assert len(issues) == 2
    # Intended failures of test data by index.
    assert issues == [
        IssueLocator(CINTable.Assessments, AssessmentFactors, 1),
        IssueLocator(CINTable.Assessments, AssessmentFactors, 2),
    ]

    # Checks rule code and message are correct.
    assert result.definition.code == "8945Q"
    assert (
        result.definition.message
        == "Please check and either amend data or provide a reason: the assessment factors code '18A' should not be used ('18B' or '18C' should be used instead)"
    )
