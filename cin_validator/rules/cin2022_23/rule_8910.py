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
LatestCategoryOfAbuse = ChildProtectionPlans.LatestCategoryOfAbuse


# define characteristics of rule
@rule_definition(
    code="8910",
    module=CINTable.ChildProtectionPlans,
    message="Latest Category of Abuse code missing or invalid (see Category of Abuse table in CIN Census code set)",
    affected_fields=[LatestCategoryOfAbuse],
)
def validate(
    data_container: Mapping[CINTable, pd.DataFrame], rule_context: RuleContext
):
    df = data_container[ChildProtectionPlans]

    # Where a Child Protection Plan module is present, <LatestCategoryOfAbuse> (N00114) must be present and be a valid code
    abuse_cats = ["NEG", "PHY", "SAB", "EMO", "MUL"]

    # Initial Category Code is not in list.
    df = df[
        (~df["LatestCategoryOfAbuse"].isin(abuse_cats))
        | df["LatestCategoryOfAbuse"].isna()
    ]

    failing_indices = df.index

    rule_context.push_issue(
        table=ChildProtectionPlans, field=LatestCategoryOfAbuse, row=failing_indices
    )


def test_validate():
    fake_cats = ["NEG", "AAA", "BBB", pd.NA, "PHY", "EMO", "MUL"]

    fake_dataframe = pd.DataFrame({"LatestCategoryOfAbuse": fake_cats})

    result = run_rule(validate, {ChildProtectionPlans: fake_dataframe})

    issues = list(result.issues)

    assert len(issues) == 3

    assert issues == [
        IssueLocator(CINTable.ChildProtectionPlans, LatestCategoryOfAbuse, 1),
        IssueLocator(CINTable.ChildProtectionPlans, LatestCategoryOfAbuse, 2),
        IssueLocator(CINTable.ChildProtectionPlans, LatestCategoryOfAbuse, 3),
    ]

    assert result.definition.code == "8910"
    assert (
        result.definition.message
        == "Latest Category of Abuse code missing or invalid (see Category of Abuse table in CIN Census code set)"
    )
