from typing import Mapping

import pandas as pd

from cin_validator.rule_engine import (
    CINTable,
    IssueLocator,
    RuleContext,
    rule_definition,
)
from cin_validator.test_engine import run_rule

ChildProtectionPlans = CINTable.ChildProtectionPlans
InitialCategoryOfAbuse = ChildProtectionPlans.InitialCategoryOfAbuse


# define characteristics of rule
@rule_definition(
    code="8905",
    module=CINTable.ChildProtectionPlans,
    message="Initial Category of Abuse code missing or invalid (see Category of Abuse table in CIN Census code set)",
    affected_fields=[InitialCategoryOfAbuse],
)
def validate(
    data_container: Mapping[CINTable, pd.DataFrame], rule_context: RuleContext
):
    df = data_container[ChildProtectionPlans]
    """
    Where a Child Protection Plan module is present, <InitialCategoryOfAbuse> (N00113) must be present and be a valid code
    """
    abuse_cats = ["NEG", "PHY", "SAB", "EMO", "MUL"]

    # Initial Category Code is not in list.
    df = df[
        (~df[InitialCategoryOfAbuse].isin(abuse_cats))
        | df[InitialCategoryOfAbuse].isna()
    ]

    failing_indices = df.index

    rule_context.push_issue(
        table=ChildProtectionPlans, field=InitialCategoryOfAbuse, row=failing_indices
    )


def test_validate():
    fake_cats = ["NEG", "AAA", "PHY", pd.NA, "BBB", "EMO", "MUL"]

    fake_dataframe = pd.DataFrame({InitialCategoryOfAbuse: fake_cats})

    result = run_rule(validate, {ChildProtectionPlans: fake_dataframe})

    issues = list(result.issues)

    assert len(issues) == 3

    assert issues == [
        IssueLocator(CINTable.ChildProtectionPlans, InitialCategoryOfAbuse, 1),
        IssueLocator(CINTable.ChildProtectionPlans, InitialCategoryOfAbuse, 3),
        IssueLocator(CINTable.ChildProtectionPlans, InitialCategoryOfAbuse, 4),
    ]

    assert result.definition.code == "8905"
    assert (
        result.definition.message
        == "Initial Category of Abuse code missing or invalid (see Category of Abuse table in CIN Census code set)"
    )
