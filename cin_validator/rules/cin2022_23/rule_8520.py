from typing import Mapping

import pandas as pd

from cin_validator.rule_engine import (
    CINTable,
    IssueLocator,
    RuleContext,
    rule_definition,
)
from cin_validator.test_engine import run_rule
from cin_validator.utils import make_census_period

ChildIdentifiers = CINTable.ChildIdentifiers
Header = CINTable.Header
PersonBirthDate = ChildIdentifiers.PersonBirthDate
ReferenceDate = Header.ReferenceDate


@rule_definition(
    code="8520",
    module=CINTable.ChildIdentifiers,
    message="Date of Birth is after data collection period (must be on or before the end of the census period)",
    affected_fields=[PersonBirthDate, ReferenceDate],
)
def validate(
    data_container: Mapping[CINTable, pd.DataFrame], rule_context: RuleContext
):
    df = data_container[ChildIdentifiers]
    df_ref = data_container[Header]

    ref_data_series = df_ref[ReferenceDate]
    collection_start, collection_end = make_census_period(ref_data_series)

    # <PersonBirthDate> (N00066) must be on or before <ReferenceDate> (N00603) or null

    df = df[(df[PersonBirthDate] > collection_end)]

    failing_indices = df.index

    rule_context.push_issue(
        table=ChildIdentifiers, field=PersonBirthDate, row=failing_indices
    )


def test_validate():
    test_dobs = pd.to_datetime(
        [
            "01/06/2021",  # 0 pass
            "01/06/2022",  # 1 fail
            "31/08/2021",  # 2 pass
            "31/12/2022",  # 3 fail
            pd.NA,  # 4 ignored
        ],
        format="%d/%m/%Y",
        errors="coerce",
    )

    fake_dobs = pd.DataFrame({PersonBirthDate: test_dobs})
    fake_header = pd.DataFrame({ReferenceDate: ["31/03/2022"]})

    result = run_rule(validate, {ChildIdentifiers: fake_dobs, Header: fake_header})

    issues = list(result.issues)
    assert len(issues) == 2
    assert issues == [
        IssueLocator(CINTable.ChildIdentifiers, PersonBirthDate, 1),
        IssueLocator(CINTable.ChildIdentifiers, PersonBirthDate, 3),
    ]

    assert result.definition.code == "8520"
    assert (
        result.definition.message
        == "Date of Birth is after data collection period (must be on or before the end of the census period)"
    )
