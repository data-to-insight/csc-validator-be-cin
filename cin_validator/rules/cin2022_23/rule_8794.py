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
    code="8794",
    module=CINTable.Disabilities,
    message="Child has two or more disabilities with the same code",
    affected_fields=[Disability],
)
def validate(
    data_container: Mapping[CINTable, pd.DataFrame], rule_context: RuleContext
):
    df = data_container[Disabilities]

    # If there is more than one <Disability> (N00099) for a child, then none of the values should appear more than once
    df_orig = df.copy()
    df_orig.reset_index(inplace=True)

    # Create a 'counts' column, a count of rows partitioned by LAchildID and Disability such that
    # if an LAchildID-Disability combination appears twice, then it'll have a count of 2, and so on.
    df = df.groupby(["LAchildID", "Disability"]).size().reset_index(name="counts")

    # Add the count column back into the original dataframe, joining by LAchildID and Disability
    df = df.merge(df_orig, how="left", on=["LAchildID", "Disability"])

    # Hold all counts >= 2 (which are the error rows)
    df = df[df["counts"] >= 2]

    # Return original index for the error rows
    failing_indices = df.set_index("index").index

    rule_context.push_issue(table=Disabilities, field=Disability, row=failing_indices)


def test_validate():
    # 0     #1      #2      #3     #4      #5      #6      #7   #8     #9   #10   #11   #12
    ids = ["1", "1", "2", "3", "3", "4", "4", "5", "5", "6", "6", "6", "6"]
    dis_is = [
        "AAA",
        "AAA",
        "BBB",
        pd.NA,
        "MOTH",
        "AAAA",
        "AAAA",
        "AA",
        "BB",
        "AA",
        "AA",
        "CC",
        "CC",
    ]

    fake_dataframe = pd.DataFrame({"LAchildID": ids, "Disability": dis_is})

    result = run_rule(validate, {Disabilities: fake_dataframe})

    issues = list(result.issues)

    assert len(issues) == 8

    assert issues == [
        IssueLocator(CINTable.Disabilities, Disability, 0),
        IssueLocator(CINTable.Disabilities, Disability, 1),
        IssueLocator(CINTable.Disabilities, Disability, 5),
        IssueLocator(CINTable.Disabilities, Disability, 6),
        IssueLocator(CINTable.Disabilities, Disability, 9),
        IssueLocator(CINTable.Disabilities, Disability, 10),
        IssueLocator(CINTable.Disabilities, Disability, 11),
        IssueLocator(CINTable.Disabilities, Disability, 12),
    ]

    assert result.definition.code == "8794"
    assert (
        result.definition.message
        == "Child has two or more disabilities with the same code"
    )
