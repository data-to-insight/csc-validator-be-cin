from typing import Mapping

import pandas as pd

from cin_validator.rule_engine import (
    CINTable,
    IssueLocator,
    RuleContext,
    rule_definition,
)
from cin_validator.test_engine import run_rule

Disabilities = CINTable.Disabilities
Disability = Disabilities.Disability


# define characteristics of rule
@rule_definition(
    code="8790",
    module=CINTable.Disabilities,
    message="Disability information includes both None and other values",
    affected_fields=[Disability],
)
def validate(
    data_container: Mapping[CINTable, pd.DataFrame], rule_context: RuleContext
):
    df = data_container[Disabilities]
    """
    If a <Disability> (N00099) value of NONE is present, then there should be 
    no other <Disability> (N00099) values present for the same child
    """

    df.reset_index(inplace=True)
    df_orig = df.copy()
    # Form df containing only the NONE disability values
    df = df[df["Disability"].str.upper() == "NONE"]

    # Join back to the original dataframe via LAchildID
    df = df.merge(df_orig, how="left", on="LAchildID", suffixes=["", "_orig"])

    # Any disability values that aren't NONE in the merged table are now an error.
    df = df[df["Disability_orig"].str.upper() != "NONE"]

    # Return original index for the error rows
    failing_indices = df.set_index("index_orig").index

    rule_context.push_issue(table=Disabilities, field=Disability, row=failing_indices)


def test_validate():
    # 0      #1      #2      #3     #4      #5      #6      #7
    ids = ["1", "1", "2", "3", "3", "4", "4", "5"]
    dis_is = ["NONE", "AIND", "NONE", pd.NA, "MOTH", "NONE", "AAAA", "AA"]

    fake_dataframe = pd.DataFrame({"LAchildID": ids, "Disability": dis_is})

    result = run_rule(validate, {Disabilities: fake_dataframe})

    issues = list(result.issues)

    assert len(issues) == 2

    assert issues == [
        IssueLocator(CINTable.Disabilities, Disability, 1),
        IssueLocator(CINTable.Disabilities, Disability, 6),
    ]

    assert result.definition.code == "8790"
    assert (
        result.definition.message
        == "Disability information includes both None and other values"
    )
