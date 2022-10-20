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
from cin_validator.utils import make_census_period

ChildIdentifiers = CINTable.ChildIdentifiers
Header = CINTable.Header
PersonDeathDate = ChildIdentifiers.PersonDeathDate
Year = Header.Year

# define characteristics of rule
@rule_definition(
    code="8545Q",
    module=CINTable.ChildIdentifiers,
    rule_type=RuleType.QUERY,
    message="Please check: Child's date of death should be within the census year",
    affected_fields=[PersonDeathDate],
)
def validate(
    data_container: Mapping[CINTable, pd.DataFrame], rule_context: RuleContext
):
    df = data_container[ChildIdentifiers]
    df_ref = data_container[Header]

    """
    If present, <PersonDeathDate> (N00108) must be within [Period_of_Census]
    """

    df = df[[PersonDeathDate]]

    # Death date must not be null, invalid text dates are made null in the line above
    df = df[df[PersonDeathDate].notna()]

    collection_year = df_ref[Year]
    collection_start, collection_end = make_census_period(collection_year)
    # DeathDate isn't in the financial year
    df = df[
        ~(
            (df[PersonDeathDate] >= collection_start)
            & (df[PersonDeathDate] <= collection_end)
        )
    ]

    failing_indices = df.index

    rule_context.push_issue(
        table=ChildIdentifiers, field=PersonDeathDate, row=failing_indices
    )


def test_validate():

    p_death = pd.to_datetime(
        [
            "2022-04-25",
            "2022-03-01",
            "2022-12-25",
            "2021-04-27",
            "2021-11-21",
            "2021-08-20",
            "2020-04-17",
            "1666-55-55",
            pd.NA,
        ],
        format="%Y/%m/%d",
        errors="coerce",
    )

    fake_ident = pd.DataFrame({PersonDeathDate: p_death})
    fake_head = pd.DataFrame({Year: ["2022"]})

    result = run_rule(validate, {ChildIdentifiers: fake_ident, Header: fake_head})

    issues = list(result.issues)

    assert len(issues) == 3

    assert issues == [
        IssueLocator(CINTable.ChildIdentifiers, PersonDeathDate, 0),
        IssueLocator(CINTable.ChildIdentifiers, PersonDeathDate, 2),
        IssueLocator(CINTable.ChildIdentifiers, PersonDeathDate, 6),
    ]

    assert result.definition.code == "8545Q"
    assert (
        result.definition.message
        == "Please check: Child's date of death should be within the census year"
    )
