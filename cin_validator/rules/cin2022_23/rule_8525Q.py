from typing import Mapping

import pandas as pd

from cin_validator.rule_engine import CINTable, RuleContext, RuleType, rule_definition
from cin_validator.rules.cin2022_23.rule_8535Q import PersonDeathDate
from cin_validator.test_engine import run_rule

ChildIdentifiers = CINTable.ChildIdentifiers
PersonBirthDate = ChildIdentifiers.PersonBirthDate
ExpectedPersonBirthDate = ChildIdentifiers.ExpectedPersonBirthDate
LAchildID = ChildIdentifiers.LAchildID


@rule_definition(
    code="8525Q",
    module=CINTable.ChildIdentifiers,
    rule_type=RuleType.QUERY,
    message="Either Date of Birth or Expected Date of Birth must be provided (but not both)",
    affected_fields=[PersonBirthDate, ExpectedPersonBirthDate],
)
def validate(
    data_container: Mapping[CINTable, pd.DataFrame], rule_context: RuleContext
):
    df = data_container[ChildIdentifiers]
    df = df.drop(columns=["ROW_ID"], errors="ignore")
    df.index.name = "ROW_ID"

    # Either Date of Birth or Expected Date of Birth must be provided (but not both)
    # condition_1 = (df[PersonBirthDate].isna() & df[ExpectedPersonBirthDate].isna())

    # condition_1 = (df[PersonBirthDate].isna()) & (df[ExpectedPersonBirthDate].isna())
    # condition_2 = df[PersonBirthDate].notna() & df[ExpectedPersonBirthDate].notna()
    mega_condition = (
        df[PersonBirthDate].isna() & df[ExpectedPersonBirthDate].notna()
    ) | (df[PersonBirthDate].notna() & df[ExpectedPersonBirthDate].isna())

    df_issues = df[~mega_condition].reset_index()

    # (LAchildID,PersonBirthDate,ExpectedPersonBirthDate) could have been used. However, in some failing conditions,
    # both (PersonBirthDate,ExpectedPersonBirthDate) can be null so their combination does not serve as a unique ID.
    # Since this is the ChildIdentifiers table and LAchildID is typically unique in it. We use that to serve as a last resort ID.

    link_id = tuple(
        zip(
            df_issues[LAchildID],
        )
    )
    df_issues["ERROR_ID"] = link_id
    df_issues = (
        df_issues.groupby("ERROR_ID", group_keys=False)["ROW_ID"]
        .apply(list)
        .reset_index()
    )

    rule_context.push_type_1(
        table=ChildIdentifiers,
        columns=[PersonBirthDate, ExpectedPersonBirthDate],
        row_df=df_issues,
    )


def test_validate():
    fake_data_frame = pd.DataFrame(
        [
            {
                "LAchildID": "child1",
                "PersonBirthDate": "26/05/2000",
                "ExpectedPersonBirthDate": "26/05/2000",
            },  # Fails because both DOB and expected DOB are present
            {
                "LAchildID": "child2",
                "PersonBirthDate": "26/05/2000",
                "ExpectedPersonBirthDate": "26/05/2001",
            },  # Fails because both DOB and expected DOB are present
            {
                "LAchildID": "child4",
                "PersonBirthDate": pd.NA,
                "ExpectedPersonBirthDate": "26/05/1999",
            },
            {
                "LAchildID": "child4",
                "PersonBirthDate": "26/05/2000",
                "ExpectedPersonBirthDate": pd.NA,
            },
            {
                "LAchildID": "child5",
                "PersonBirthDate": "26/05/2000",
                "ExpectedPersonBirthDate": "25/05/2000",
            },  # Fails because both DOB and expected DOB are present
            {
                "LAchildID": "child6",
                "PersonBirthDate": pd.NA,
                "ExpectedPersonBirthDate": pd.NA,
            },  # Fails because there is no DOB or expected DOB
        ]
    )

    result = run_rule(validate, {ChildIdentifiers: fake_data_frame})

    issues = result.type1_issues

    issue_table = issues.table
    assert issue_table == ChildIdentifiers

    issue_columns = issues.columns
    assert issue_columns == [PersonBirthDate, ExpectedPersonBirthDate]

    issue_rows = issues.row_df
    assert len(issue_rows) == 4
    assert isinstance(issue_rows, pd.DataFrame)
    assert issue_rows.columns.to_list() == ["ERROR_ID", "ROW_ID"]

    expected_df = pd.DataFrame(
        [
            {
                "ERROR_ID": ("child1",),
                "ROW_ID": [0],
            },
            {
                "ERROR_ID": ("child2",),
                "ROW_ID": [1],
            },
            {
                "ERROR_ID": ("child5",),
                "ROW_ID": [4],
            },
            {
                "ERROR_ID": ("child6",),
                "ROW_ID": [5],
            },
        ]
    )
    assert issue_rows.equals(expected_df)

    assert result.definition.code == "8525Q"
    assert (
        result.definition.message
        == "Either Date of Birth or Expected Date of Birth must be provided (but not both)"
    )
