from typing import Mapping

import pandas as pd

from cin_validator.rule_engine import CINTable, RuleContext, rule_definition
from cin_validator.test_engine import run_rule

Assessments = CINTable.Assessments
LAchildID = Assessments.LAchildID
AssessmentActualStartDate = Assessments.AssessmentActualStartDate
AssessmentAuthorisationDate = Assessments.AssessmentAuthorisationDate


@rule_definition(
    code="8608",
    module=CINTable.Assessments,
    message="Assessment Start Date cannot be later than its End Date",
    affected_fields=[AssessmentActualStartDate, AssessmentAuthorisationDate],
)
def validate(
    data_container: Mapping[CINTable, pd.DataFrame], rule_context: RuleContext
):
    df = data_container[Assessments]
    df.index.name = "ROW_ID"

    # If present <AssessmentAuthorisationDate> (N00160) must be on or after the <AssessmentActualStartDate> (N00159)
    condition = df[AssessmentActualStartDate] > df[AssessmentAuthorisationDate]

    df_issues = df[condition].reset_index()

    link_id = tuple(
        zip(
            df_issues[LAchildID],
            df_issues[AssessmentActualStartDate],
            df_issues[AssessmentAuthorisationDate],
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
        columns=[AssessmentActualStartDate, AssessmentAuthorisationDate],
        row_df=df_issues,
    )


def test_validate():
    assessments = pd.DataFrame(
        [
            {
                "LAchildID": "child1",
                "AssessmentActualStartDate": "26/05/2000",
                "AssessmentAuthorisationDate": "26/05/2000",
            },
            {
                "LAchildID": "child2",
                "AssessmentActualStartDate": "26/05/2000",
                "AssessmentAuthorisationDate": "26/05/2001",
            },
            {
                "LAchildID": "child3",
                "AssessmentActualStartDate": "26/05/2000",
                "AssessmentAuthorisationDate": "26/05/1999",
            },  # 2 error: end is before start
            {
                "LAchildID": "child3",
                "AssessmentActualStartDate": "26/05/2000",
                "AssessmentAuthorisationDate": pd.NA,
            },
            {
                "LAchildID": "child4",
                "AssessmentActualStartDate": "26/05/2000",
                "AssessmentAuthorisationDate": "25/05/2000",
            },  # 4 error: end is before start
            {
                "LAchildID": "child5",
                "AssessmentActualStartDate": pd.NA,
                "AssessmentAuthorisationDate": pd.NA,
            },
        ]
    )

    assessments[AssessmentActualStartDate] = pd.to_datetime(
        assessments[AssessmentActualStartDate], format="%d/%m/%Y", errors="coerce"
    )
    assessments[AssessmentAuthorisationDate] = pd.to_datetime(
        assessments[AssessmentAuthorisationDate], format="%d/%m/%Y", errors="coerce"
    )

    result = run_rule(validate, {Assessments: assessments})

    issues = result.type1_issues

    issue_table = issues.table
    assert issue_table == Assessments

    issue_columns = issues.columns
    assert issue_columns == [AssessmentActualStartDate, AssessmentAuthorisationDate]

    issue_rows = issues.row_df
    assert len(issue_rows) == 2
    assert isinstance(issue_rows, pd.DataFrame)
    assert issue_rows.columns.to_list() == ["ERROR_ID", "ROW_ID"]

    expected_df = pd.DataFrame(
        [
            {
                "ERROR_ID": (
                    "child3",
                    pd.to_datetime("26/05/2000", format="%d/%m/%Y", errors="coerce"),
                    pd.to_datetime("26/05/1999", format="%d/%m/%Y", errors="coerce"),
                ),
                "ROW_ID": [2],
            },
            {
                "ERROR_ID": (
                    "child4",
                    pd.to_datetime("26/05/2000", format="%d/%m/%Y", errors="coerce"),
                    pd.to_datetime("25/05/2000", format="%d/%m/%Y", errors="coerce"),
                ),
                "ROW_ID": [4],
            },
        ]
    )
    assert issue_rows.equals(expected_df)

    assert result.definition.code == "8608"
    assert (
        result.definition.message
        == "Assessment Start Date cannot be later than its End Date"
    )
