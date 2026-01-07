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

PreProceedings = CINTable.PreProceedings
PPStartDate = PreProceedings.PPStartDate

Header = CINTable.Header
ReferenceDate = Header.ReferenceDate


@rule_definition(
    code="9005",
    module=CINTable.PreProceedings,
    message="The date of the legal planning meeting or other decision making forum where the LA made the decision to commence pre-proceedings cannot fall outside the reporting year.",
    affected_fields=[PPStartDate],
)
def validate(
    data_container: Mapping[CINTable, pd.DataFrame], rule_context: RuleContext
):
    # If (N00826) is present, then (N00826) must fall within [Period_of_Census] inclusive.
    df = data_container[PreProceedings]

    df_ref = data_container[Header]
    ref_date_series = df_ref[ReferenceDate]
    collection_start, collection_end = make_census_period(ref_date_series)

    df_issues = df[
        (df[PPStartDate] > collection_end) | (df[PPStartDate] < collection_start)
    ]

    failing_indices = df_issues.index

    rule_context.push_issue(
        table=PreProceedings, field=PPStartDate, row=failing_indices
    )


def test_validate():
    sample_header = pd.DataFrame(
        [{ReferenceDate: "31/03/2001"}]  # collection_start is 01/04/2000
    )

    sample_pp = pd.DataFrame(
        {"PPStartDate": ["30/03/2000", "01/04/2000", "01/04/2001"]}
    )

    sample_pp["PPStartDate"] = pd.to_datetime(
        sample_pp["PPStartDate"], dayfirst=True, errors="coerce"
    )

    result = run_rule(
        validate,
        {
            Header: sample_header,
            PreProceedings: sample_pp,
        },
    )

    issues = list(result.issues)

    assert len(issues) == 2

    assert issues == [
        IssueLocator(CINTable.PreProceedings, PPStartDate, 0),
        IssueLocator(CINTable.PreProceedings, PPStartDate, 2),
    ]

    assert result.definition.code == "9005"
    assert (
        result.definition.message
        == "The date of the legal planning meeting or other decision making forum where the LA made the decision to commence pre-proceedings cannot fall outside the reporting year."
    )
