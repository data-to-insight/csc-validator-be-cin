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

# Get tables and columns of interest from the CINTable object defined in rule_engine/__api.py

Assessments = CINTable.Assessments
Header = CINTable.Header
AssessmentAuthorisationDate = Assessments.AssessmentAuthorisationDate
ReferenceDate = Header.ReferenceDate


# define characteristics of rule
@rule_definition(
    code="8696",
    module=CINTable.Assessments,
    message="Assessment end date must fall within the census year",
    affected_fields=[AssessmentAuthorisationDate, ReferenceDate],
)
def validate(
    data_container: Mapping[CINTable, pd.DataFrame], rule_context: RuleContext
):
    df = data_container[Assessments]
    df_ref = data_container[Header]

    ref_data_series = df_ref[ReferenceDate]
    collection_start, collection_end = make_census_period(ref_data_series)

    # implement rule logic as described by the Github issue. Put the description as a comment above the implementation as shown.

    # <If present <AssessmentAuthorisationDate> (N00160) must be on or between [Start_Of_Census_Year] and <ReferenceDate> (N00603)

    df = df[
        (df[AssessmentAuthorisationDate] < collection_start)
        | (df[AssessmentAuthorisationDate] > collection_end)
    ]

    failing_indices = df.index

    rule_context.push_issue(
        table=Assessments, field=AssessmentAuthorisationDate, row=failing_indices
    )


def test_validate():
    # Create some sample data such that some values pass the validation and some fail.

    miss_auth = pd.to_datetime(
        [
            "01/03/2019",  # 0 fail
            "01/04/2021",  # 1 pass
            "01/10/2022",  # 2 fail
            pd.NA,  # 3 ignored
        ],
        format="%d/%m/%Y",
        errors="coerce",
    )

    fake_auth = pd.DataFrame({AssessmentAuthorisationDate: miss_auth})
    fake_header = pd.DataFrame({ReferenceDate: ["31/03/2022"]})

    # Run rule function passing in our sample data
    result = run_rule(validate, {Assessments: fake_auth, Header: fake_header})

    # The result contains a list of issues encountered
    issues = list(result.issues)
    # replace 2 with the number of failing points you expect from the sample data.
    assert len(issues) == 2
    # replace the table and column name as done earlier.
    # The last numbers represent the index values where you expect the sample data to fail the validation check.
    assert issues == [
        IssueLocator(CINTable.Assessments, AssessmentAuthorisationDate, 0),
        IssueLocator(CINTable.Assessments, AssessmentAuthorisationDate, 2),
    ]

    # Check that the rule definition is what you wrote in the context above.

    assert result.definition.code == "8696"
    assert (
        result.definition.message
        == "Assessment end date must fall within the census year"
    )
