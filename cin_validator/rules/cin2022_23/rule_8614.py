from typing import Mapping

import pandas as pd

from cin_validator.rule_engine import CINTable, RuleContext, rule_definition
from cin_validator.test_engine import run_rule

Assessments = CINTable.Assessments
AssessmentAuthorisationDate = Assessments.AssessmentAuthorisationDate
AssessmentFactors = Assessments.AssessmentFactors
LAchildID = Assessments.LAchildID


@rule_definition(
    code="8614",
    module=CINTable.Assessments,
    message="Parental or child factors at assessment should only be present for a completed assessment.",
    affected_fields=[AssessmentAuthorisationDate, AssessmentFactors],
)
def validate(
    data_container: Mapping[CINTable, pd.DataFrame], rule_context: RuleContext
):
    df = data_container[Assessments]
    df.index.name = "ROW_ID"

    # If <AssessmentAuthorisationDate> (N00160) is missing,
    # <AssessmentFactors> (N00181) within the same module must also be missing
    # fails if AssessmentAuthoriationDate is null and AssessmentFactors  is not null
    condition = df[AssessmentAuthorisationDate].isna() & df[AssessmentFactors].notna()

    df_issues = df[condition].reset_index()

    link_id = tuple(
        zip(
            df_issues[LAchildID],
            df_issues[AssessmentAuthorisationDate],
            df_issues[AssessmentFactors],
        )
    )
    df_issues["ERROR_ID"] = link_id
    df_issues = (
        df_issues.groupby("ERROR_ID", group_keys=False)["ROW_ID"]
        .apply(list)
        .reset_index()
    )

    rule_context.push_type_1(
        table=Assessments,
        columns=[AssessmentAuthorisationDate, AssessmentFactors],
        row_df=df_issues,
    )


def test_validate():
    fake_data = pd.DataFrame(
        [
            {
                "LAchildID": "child1",
                "AssessmentAuthorisationDate": "26/05/2000",
                "AssessmentFactors": "26/05/2000",
            },
            {
                "LAchildID": "child2",
                "AssessmentAuthorisationDate": "26/05/2000",
                "AssessmentFactors": "26/05/2001",
            },
            {
                "LAchildID": "child3",
                "AssessmentAuthorisationDate": pd.NA,
                "AssessmentFactors": "26/05/2000",
            },  # error - Authorisation date is null and Assessment factors is not
            {
                "LAchildID": "child3",
                "AssessmentAuthorisationDate": "26/05/2000",
                "AssessmentFactors": pd.NA,
            },
            {
                "LAchildID": "child4",
                "AssessmentAuthorisationDate": pd.NA,
                "AssessmentFactors": "25/05/2000",
            },  # error Authorisation date is null and assessment factors is not
            {
                "LAchildID": "child5",
                "AssessmentAuthorisationDate": pd.NA,
                "AssessmentFactors": pd.NA,
            },
        ]
    )

    result = run_rule(validate, {Assessments: fake_data})

    issues = result.type1_issues

    issue_table = issues.table
    assert issue_table == Assessments

    issue_columns = issues.columns
    assert issue_columns == [AssessmentAuthorisationDate, AssessmentFactors]

    issue_rows = issues.row_df
    assert len(issue_rows) == 2
    assert isinstance(issue_rows, pd.DataFrame)
    assert issue_rows.columns.to_list() == ["ERROR_ID", "ROW_ID"]
    expected_df = pd.DataFrame(
        [
            {
                "ERROR_ID": (
                    "child3",
                    pd.NA,
                    "26/05/2000",
                ),
                "ROW_ID": [2],
            },
            {
                "ERROR_ID": (
                    "child4",
                    pd.NA,
                    "25/05/2000",
                ),
                "ROW_ID": [4],
            },
        ]
    )
    assert issue_rows.equals(expected_df)

    assert result.definition.code == "8614"
    assert (
        result.definition.message
        == "Parental or child factors at assessment should only be present for a completed assessment."
    )
