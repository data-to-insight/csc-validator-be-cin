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
PrimaryNeedCode = CINdetails.PrimaryNeedCode


# define characteristics of rule
@rule_definition(
    code="8650",
    module=CINTable.CINdetails,
    message="Primary Need Code invalid (see Primary Need table in CIN census code set)",
    affected_fields=[PrimaryNeedCode],
)
def validate(
    data_container: Mapping[CINTable, pd.DataFrame], rule_context: RuleContext
):
    df = data_container[CINdetails]

    # If present <PrimaryNeedCode> (N00101) must be a valid primary need code value
    PriNeed_list = ["N0", "N1", "N2", "N3", "N4", "N5", "N6", "N7", "N8", "N9"]

    # Primary Need Code is not in list.
    df = df[(~df["PrimaryNeedCode"].isin(PriNeed_list)) & df["PrimaryNeedCode"].notna()]

    failing_indices = df.index

    rule_context.push_issue(
        table=CINdetails, field=PrimaryNeedCode, row=failing_indices
    )


def test_validate():
    pri_need = ["N1", "N8", "AA", pd.NA, "N6", "BB", "N9"]

    fake_dataframe = pd.DataFrame({"PrimaryNeedCode": pri_need})

    result = run_rule(validate, {CINdetails: fake_dataframe})

    issues = list(result.issues)

    assert len(issues) == 2

    assert issues == [
        IssueLocator(CINTable.CINdetails, PrimaryNeedCode, 2),
        IssueLocator(CINTable.CINdetails, PrimaryNeedCode, 5),
    ]

    assert result.definition.code == "8650"
    assert (
        result.definition.message
        == "Primary Need Code invalid (see Primary Need table in CIN census code set)"
    )
