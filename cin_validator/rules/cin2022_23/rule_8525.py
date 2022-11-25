from typing import Mapping

import pandas as pd

from cin_validator.rule_engine import CINTable, RuleContext, rule_definition
from cin_validator.rules.cin2022_23.rule_8535Q import PersonDeathDate
from cin_validator.test_engine import run_rule

# Get tables and columns of interest from the CINTable object defined in rule_engine/__api.py


ChildIdentifiers = CINTable.ChildIdentifiers
PersonBirthDate = ChildIdentifiers.PersonBirthDate
ExpectedPersonBirthDate = ChildIdentifiers.ExpectedPersonBirthDate
LAchildID = ChildIdentifiers.LAchildID

# define characteristics of rule
@rule_definition(
    # write the rule code here, in place of 8500
    code=8525,
    # replace ChildIdentifiers with the value in the module column of the excel sheet corresponding to this rule .
    module=CINTable.ChildIdentifiers,
    # replace the message with the corresponding value for this rule, gotten from the excel sheet.
    message="Either Date of Birth or Expected Date of Birth must be provided (but not both)",
    # The column names tend to be the words within the < > signs in the github issue description.
    affected_fields=[PersonBirthDate, ExpectedPersonBirthDate],
)
def validate(
    data_container: Mapping[CINTable, pd.DataFrame], rule_context: RuleContext
):
    # PREPARING DATA

    # Replace ChildIdentifiers with the name of the table you need.
    df = data_container[ChildIdentifiers]
    # Before you begin, rename the index so that the initial row positions can be kept intact.
    df.index.name = "ROW_ID"

    # lOGIC
    # Implement rule logic as described by the Github issue.
    # Put the description as a comment above the implementation as shown.

    # Either Date of Birth or Expected Date of Birth must be provided (but not both)
    # condition_1 = (df[PersonBirthDate].isna() & df[ExpectedPersonBirthDate].isna())
    condition_1 = (df[PersonBirthDate].isna()) & (df[ExpectedPersonBirthDate].isna())
    condition_2 = df[PersonBirthDate].notna() & df[ExpectedPersonBirthDate].notna()

    # get all the data that fits the failing condition. Reset the index so that ROW_ID now becomes a column of df
    df_issues = df[condition_1 | condition_2].reset_index()

    # (LAchildID,PersonBirthDate,ExpectedPersonBirthDate) could have been used. However, in some failing conditions,
    # both (PersonBirthDate,ExpectedPersonBirthDate) can be null so their combination does not serve as a unique ID.
    # Since this is the ChildIdentifiers table and LAchildID is typically unique in it. We use that to serve as a last resort ID.

    link_id = tuple(
        zip(
            df_issues[LAchildID],
        )
    )
    df_issues["ERROR_ID"] = link_id
    df_issues = df_issues.groupby("ERROR_ID")["ROW_ID"].apply(list).reset_index()
    # Ensure that you do not change the ROW_ID, and ERROR_ID column names which are shown above. They are keywords in this project.
    rule_context.push_type_1(
        table=ChildIdentifiers,
        columns=[PersonBirthDate, ExpectedPersonBirthDate],
        row_df=df_issues,
    )


def test_validate():
    # Create some sample data such that some values pass the validation and some fail.
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
    # Date values not checked so no datetime conversion

    # Run rule function passing in our sample data
    result = run_rule(validate, {ChildIdentifiers: fake_data_frame})

    # Use .type1_issues to check for the result of .push_type1_issues() which you used above.
    issues = result.type1_issues

    # get table name and check it. Replace ChildProtectionPlans with the name of your table.
    issue_table = issues.table
    assert issue_table == ChildIdentifiers

    # check that the right columns were returned. Replace CPPstartDate and CPPendDate with a list of your columns.
    issue_columns = issues.columns
    assert issue_columns == [PersonBirthDate, ExpectedPersonBirthDate]

    # check that the location linking dataframe was formed properly.
    issue_rows = issues.row_df
    # replace 2 with the number of failing points you expect from the sample data.
    assert len(issue_rows) == 4
    # check that the failing locations are contained in a DataFrame having the appropriate columns. These lines do not change.
    assert isinstance(issue_rows, pd.DataFrame)
    assert issue_rows.columns.to_list() == ["ERROR_ID", "ROW_ID"]

    # Create the dataframe which you expect, based on the fake data you created. It should have two columns.
    # - The first column is ERROR_ID which contains the unique combination that identifies each error instance, which you decided on earlier.
    # - The second column in ROW_ID which contains a list of index positions that belong to each error instance.

    # The ROW ID values represent the index positions where you expect the sample data to fail the validation check.
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

    # Check that the rule definition is what you wrote in the context above.

    # replace 8925 with the rule code and put the appropriate message in its place too.
    assert result.definition.code == 8525
    assert (
        result.definition.message
        == "Either Date of Birth or Expected Date of Birth must be provided (but not both)"
    )
