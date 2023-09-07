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
ReferralNFA = CINdetails.ReferralNFA


@rule_definition(
    code="8568",
    module=CINTable.CINdetails,
    message="RNFA flag is missing or invalid",
    affected_fields=[ReferralNFA],
)
def validate(
    data_container: Mapping[CINTable, pd.DataFrame], rule_context: RuleContext
):
    df = data_container[CINdetails]

    # <ReferralNFA> (N00112) must be present and must be 1 or 0, true or false
    df["ReferralNFA"] = df["ReferralNFA"].astype(str).str.lower()
    df = df[~df["ReferralNFA"].isin(["1", "0", "false", "true"])]

    failing_indices = df.index

    rule_context.push_issue(table=CINdetails, field=ReferralNFA, row=failing_indices)


def test_validate():
    RNFA = [1, 0, 2, pd.NA, "true", "Woof", "Meow"]

    fake_dataframe = pd.DataFrame({"ReferralNFA": RNFA})

    result = run_rule(validate, {CINdetails: fake_dataframe})

    issues = list(result.issues)

    assert len(issues) == 4

    assert issues == [
        IssueLocator(CINTable.CINdetails, ReferralNFA, 2),
        IssueLocator(CINTable.CINdetails, ReferralNFA, 3),
        IssueLocator(CINTable.CINdetails, ReferralNFA, 5),
        IssueLocator(CINTable.CINdetails, ReferralNFA, 6),
    ]

    assert result.definition.code == "8568"
    assert result.definition.message == "RNFA flag is missing or invalid"
