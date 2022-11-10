from typing import Mapping

import pandas as pd

from cin_validator.rule_engine import CINTable, RuleContext, RuleType, rule_definition
from cin_validator.test_engine import run_rule

# Get tables and columns of interest from the CINTable object defined in rule_engine/__api.py

ChildIdentifiers = CINTable.ChildIdentifiers
PersonBirthDate = ChildIdentifiers.PersonBirthDate
PersonDeathDate = ChildIdentifiers.PersonDeathDate
LAchildID = ChildIdentifiers.LAchildID

# define characteristics of rule
@rule_definition(
    # write the rule code here, in place of 8500
    code="8535Q",
    # replace ChildIdentifiers with the value in the module column of the excel sheet corresponding to this rule .
    module=CINTable.ChildIdentifiers,
    # specify that it is a query
    rule_type=RuleType.QUERY,
    # replace the message with the corresponding value for this rule, gotten from the excel sheet.
    message="Child’s date of death should not be prior to the date of birth",
    # The column names tend to be the words within the < > signs in the github issue description.
    affected_fields=[PersonDeathDate, PersonBirthDate],
)
def validate(
    data_container: Mapping[CINTable, pd.DataFrame], rule_context: RuleContext
):
    # PREPARING DATA

    # Replace ChildIdentifiers with the name of the table you need.
    df = data_container[ChildIdentifiers]
    # Before you begine, rename the index so that the initial row positions can be kept intact.
    df.index.name = "ROW_ID"

    # lOGIC
    # Implement rule logic as described by the Github issue.
    # Put the description as a comment above the implementation as shown.

    # <PersonDeathDate> (N00108) must be on or after <PersonBirthDate> (N00066)

    # Remove all rows with no deathdate
    df = df[~df[PersonDeathDate].isna()]

    # Return rows where DOB is prior to DOD
    condition1 = df[PersonBirthDate] < df[PersonDeathDate]
    # Return rows with no DOB
    condition2 = df[PersonBirthDate].isna()

    # df with all rows meeting the conditions
    df_issues = df[condition1 | condition2].reset_index()

    link_id = tuple(
        zip(
            df_issues[LAchildID], df_issues[PersonDeathDate], df_issues[PersonBirthDate]
        )
    )
    df_issues["ERROR_ID"] = link_id
    df_issues = df_issues.groupby("ERROR_ID", group_keys=False)["ROW_ID"].apply(list).reset_index()
    # Ensure that you do not change the ROW_ID, and ERROR_ID column names which are shown above. They are keywords in this project.
    rule_context.push_type_1(
        table=ChildIdentifiers,
        columns=[PersonDeathDate, PersonBirthDate],
        row_df=df_issues,
    )


def test_validate():
    # Create some sample data such that some values pass the validation and some fail.
    child_identifiers = pd.DataFrame(
        [
            {
                "LAchildID": "child1",
                "PersonDeathDate": "26/05/2000",
                "PersonBirthDate": "26/05/2000",
            },
            {
                "LAchildID": "child2",
                "PersonDeathDate": "26/05/2000",
                "PersonBirthDate": "26/05/2001",
            },
            {
                "LAchildID": "child3",
                "PersonDeathDate": "26/05/2000",
                "PersonBirthDate": "26/05/1999",
            },  # 2 error: end is before start
            {
                "LAchildID": "child4",
                "PersonDeathDate": "26/05/2000",
                "PersonBirthDate": pd.NA,
                # 3 error: no birth date
            },
            {
                "LAchildID": "child5",
                "PersonDeathDate": "26/05/2000",
                "PersonBirthDate": "25/05/2000",
            },  # 4 error: end is before start
            {
                "LAchildID": "child6",
                "PersonDeathDate": pd.NA,
                "PersonBirthDate": pd.NA,
            },
        ]
    )
    # Conver to dates
    child_identifiers[PersonDeathDate] = pd.to_datetime(
        child_identifiers[PersonDeathDate], format="%d/%m/%Y", errors="coerce"
    )
    child_identifiers[PersonBirthDate] = pd.to_datetime(
        child_identifiers[PersonBirthDate], format="%d/%m/%Y", errors="coerce"
    )

    # Run rule function passing in our sample data
    result = run_rule(validate, {ChildIdentifiers: child_identifiers})

    # Use .type1_issues to check for the result of .push_type1_issues() which you used above.
    issues = result.type1_issues

    # get table name and check it. Replace ChildIdentifiers with the name of your table.
    issue_table = issues.table
    assert issue_table == ChildIdentifiers

    # check that the right columns were returned. Replace PersonDeathDate and PersonBirthDate with a list of your columns.
    issue_columns = issues.columns
    assert issue_columns == [PersonDeathDate, PersonBirthDate]

    # check that the location linking dataframe was formed properly.
    issue_rows = issues.row_df
    # replace 2 with the number of failing points you expect from the sample data.
    assert len(issue_rows) == 3
    # check that the failing locations are contained in a DataFrame having the appropriate columns. These lines do not change.
    assert isinstance(issue_rows, pd.DataFrame)
    assert issue_rows.columns.to_list() == ["ERROR_ID", "ROW_ID"]

    # Create the dataframe which you expect, based on the fake data you created. It should have two columns.

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
                    pd.to_datetime(pd.NA, format="%d/%m/%Y", errors="coerce"),
                ),
                "ROW_ID": [3],
            },
            {
                "ERROR_ID": (
                    "child5",
                    pd.to_datetime("26/05/2000", format="%d/%m/%Y", errors="coerce"),
                    pd.to_datetime("25/05/2000", format="%d/%m/%Y", errors="coerce"),
                ),
                "ROW_ID": [4],
            },
        ]
    )
    assert issue_rows.equals(expected_df)

    # Check that the rule definition is what you wrote in the context above.

    assert result.definition.code == "8535Q"
    assert (
        result.definition.message
        == "Child’s date of death should not be prior to the date of birth"
    )
