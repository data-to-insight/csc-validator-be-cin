from typing import Mapping

import pandas as pd

from cin_validator.rule_engine import (
    CINTable,
    IssueLocator,
    RuleContext,
    rule_definition,
)
from cin_validator.test_engine import run_rule

ChildCharacteristics = CINTable.ChildCharacteristics
Ethnicity = ChildCharacteristics.Ethnicity


@rule_definition(
    code="4220",
    module=CINTable.ChildCharacteristics,
    message="Ethnicity is missing or invalid (see Ethnicity table)",
    affected_fields=[Ethnicity],
)
def validate(
    data_container: Mapping[CINTable, pd.DataFrame], rule_context: RuleContext
):
    df = data_container[ChildCharacteristics]
    """
    <Ethnicity> (N00177) must be present and a valid code
    """

    eth_list = [
        "ABAN",
        "AIND",
        "AOTH",
        "APKN",
        "BAFR",
        "BCRB",
        "BOTH",
        "CHNE",
        "MOTH",
        "MWAS",
        "MWBA",
        "MWBC",
        "NOBT",
        "OOTH",
        "REFU",
        "WBRI",
        "WIRI",
        "WIRT",
        "WOTH",
        "WROM",
    ]

    df.reset_index(inplace=True)

    # Ethnicity is not in list or is null.
    df2 = df[(~df["Ethnicity"].isin(eth_list)) | df["Ethnicity"].isna()]

    failing_indices = df2.set_index("index").index

    rule_context.push_issue(
        table=ChildCharacteristics, field=Ethnicity, row=failing_indices
    )


def test_validate():
    # 0      #1      #2      #3     #4      #5      #6
    eths = ["ABAB", "AIND", "AOTH", pd.NA, "MOTH", "WOTH", "AAAA"]

    fake_dataframe = pd.DataFrame({"Ethnicity": eths})

    result = run_rule(validate, {ChildCharacteristics: fake_dataframe})

    issues = list(result.issues)

    assert len(issues) == 3

    assert issues == [
        IssueLocator(CINTable.ChildCharacteristics, Ethnicity, 0),
        IssueLocator(CINTable.ChildCharacteristics, Ethnicity, 3),
        IssueLocator(CINTable.ChildCharacteristics, Ethnicity, 6),
    ]

    assert result.definition.code == "4220"
    assert (
        result.definition.message
        == "Ethnicity is missing or invalid (see Ethnicity table)"
    )
