from typing import Mapping

import pandas as pd

from cin_validator.rule_engine import CINTable, RuleContext, RuleType, rule_definition
from cin_validator.test_engine import run_rule

ChildIdentifiers = CINTable.ChildIdentifiers
PersonBirthDate = ChildIdentifiers.PersonBirthDate
ExpectedPersonBirthDate = ChildIdentifiers.ExpectedPersonBirthDate
PersonDeathDate = ChildIdentifiers.PersonDeathDate
LAchildID = ChildIdentifiers.LAchildID


@rule_definition(
    code="8535Q",
    module=CINTable.ChildIdentifiers,
    rule_type=RuleType.QUERY,
    message="Please check and either amend data or provide a reason: Child’s date of death should not be prior to the date of birth",
    affected_fields=[PersonDeathDate, PersonBirthDate],
)
def validate(
    data_container: Mapping[CINTable, pd.DataFrame], rule_context: RuleContext
):
    df = data_container[ChildIdentifiers]
    df.index.name = "ROW_ID"

    # <PersonDeathDate> (N00108) must be on or after <PersonBirthDate> (N00066)

    # Remove all rows with no deathdate
    df = df[~df[PersonDeathDate].isna()]
    # Remove children who died unborn. They shouldn't flag this rule [DfE tool doesn't].
    df = df[~(df[ExpectedPersonBirthDate] > df[PersonDeathDate])]

    # Return rows where DOB is prior to DOD
    condition1 = df[PersonBirthDate] > df[PersonDeathDate]
    condition2 = df[PersonBirthDate].isna()

    # df with all rows meeting the conditions
    df_issues = df[condition1 | condition2].reset_index()

    link_id = tuple(
        zip(
            df_issues[LAchildID], df_issues[PersonDeathDate], df_issues[PersonBirthDate]
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
        columns=[PersonDeathDate, PersonBirthDate],
        row_df=df_issues,
    )


def test_validate():
    child_identifiers = pd.DataFrame(
        [
            {
                "LAchildID": "child1",
                "PersonDeathDate": "26/05/2000",
                "PersonBirthDate": "26/05/2000",
            },
            {
                "LAchildID": "child2",
                "PersonDeathDate": "26/05/2001",
                "PersonBirthDate": "26/05/2000",
            },
            {
                "LAchildID": "child3",
                "PersonDeathDate": "26/05/1999",
                "PersonBirthDate": "26/05/2000",
            },  # 2 error: end is before start
            {
                "LAchildID": "child4",
                "PersonDeathDate": "26/05/2000",
                "ExpectedPersonBirthDate": "27/05/2000",
                "PersonBirthDate": pd.NA,
                # 3 pass: no birth date
            },
            {
                "LAchildID": "child5",
                "PersonDeathDate": "25/05/2000",
                "PersonBirthDate": "26/05/2000",
            },  # 4 error: end is before start
            {
                "LAchildID": "child6",
                "PersonDeathDate": pd.NA,
                "PersonBirthDate": pd.NA,
            },
        ]
    )

    child_identifiers[PersonDeathDate] = pd.to_datetime(
        child_identifiers[PersonDeathDate], format="%d/%m/%Y", errors="coerce"
    )
    child_identifiers[PersonBirthDate] = pd.to_datetime(
        child_identifiers[PersonBirthDate], format="%d/%m/%Y", errors="coerce"
    )
    child_identifiers[ExpectedPersonBirthDate] = pd.to_datetime(
        child_identifiers[ExpectedPersonBirthDate], format="%d/%m/%Y", errors="coerce"
    )

    result = run_rule(validate, {ChildIdentifiers: child_identifiers})

    issues = result.type1_issues

    issue_table = issues.table
    assert issue_table == ChildIdentifiers

    issue_columns = issues.columns
    assert issue_columns == [PersonDeathDate, PersonBirthDate]

    issue_rows = issues.row_df
    assert len(issue_rows) == 2
    assert isinstance(issue_rows, pd.DataFrame)
    assert issue_rows.columns.to_list() == ["ERROR_ID", "ROW_ID"]

    expected_df = pd.DataFrame(
        [
            {
                "ERROR_ID": (
                    "child3",
                    pd.to_datetime("26/05/1999", format="%d/%m/%Y", errors="coerce"),
                    pd.to_datetime("26/05/2000", format="%d/%m/%Y", errors="coerce"),
                ),
                "ROW_ID": [2],
            },
            {
                "ERROR_ID": (
                    "child5",
                    pd.to_datetime("25/05/2000", format="%d/%m/%Y", errors="coerce"),
                    pd.to_datetime("26/05/2000", format="%d/%m/%Y", errors="coerce"),
                ),
                "ROW_ID": [4],
            },
        ]
    )
    assert issue_rows.equals(expected_df)

    assert result.definition.code == "8535Q"
    assert (
        result.definition.message
        == "Please check and either amend data or provide a reason: Child’s date of death should not be prior to the date of birth"
    )
