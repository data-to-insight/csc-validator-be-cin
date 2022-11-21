from typing import Mapping

import pandas as pd

from cin_validator.rule_engine import CINTable, RuleContext, rule_definition
from cin_validator.test_engine import run_rule

# Get tables and columns of interest from the CINTable object defined in rule_engine/__api.py

ChildIdentifiers = CINTable.ChildIdentifiers
LAchildID = ChildIdentifiers.LAchildID
PersonBirthDate = ChildIdentifiers.PersonBirthDate
ExpectedPersonBirthDate = ChildIdentifiers.ExpectedPersonBirthDate
GenderCurrent = ChildIdentifiers.GenderCurrent

# define characteristics of rule
@rule_definition(
    # write the rule code here, in place of 8500
    code=8750,
    # replace ChildIdentifiers with the value in the module column of the excel sheet corresponding to this rule .
    module=CINTable.ChildIdentifiers,
    # replace the message with the corresponding value for this rule, gotten from the excel sheet.
    message="Gender must equal 0 for an unborn child",
    # The column names tend to be the words within the < > signs in the github issue description.
    affected_fields=[GenderCurrent, PersonBirthDate, ExpectedPersonBirthDate],
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

    # If <ExpectedPersonBirthDate> (N00098) is present and <PersonBirthDate> (N00066) is blank then <GenderCurrent> (N00065) must equal “0”
    condition = (
        df[PersonBirthDate].isna()
        & df[ExpectedPersonBirthDate].notna()
        & (df[GenderCurrent].astype(str) != "0")
    )
    # get all the data that fits the failing condition. Reset the index so that ROW_ID now becomes a column of df
    df_issues = df[condition].reset_index()

    # SUBMIT ERRORS
    # Generate a unique ID for each instance of an error. In this case,
    # - If only LAchildID is used as an identifier, multiple instances of the error on a child will be understood as 1 instance.
    # We don't want that because in reality, a child can have multiple instances of an error.
    # - If we use the LAchildID-CPPstartDate combination, that artificially cancels out the instances where a start date repeats for the same child.
    # Another rule checks for that condition. Not this one.
    # - It is very unlikely that a combination of LAchildID-CPPstartDate-CPPendDate will repeat in the DataFrame.
    # Hence, it can be used as a unique identifier of the row.

    # Replace CPPstartDate and CPPendDate below with the columns concerned in your rule.
    link_id = tuple(
        zip(
            df_issues[LAchildID],
            df_issues[GenderCurrent],
            df_issues[ExpectedPersonBirthDate],
        )
    )
    df_issues["ERROR_ID"] = link_id
    df_issues = df_issues.groupby("ERROR_ID")["ROW_ID"].apply(list).reset_index()
    # Ensure that you do not change the ROW_ID, and ERROR_ID column names which are shown above. They are keywords in this project.
    rule_context.push_type_1(
        table=ChildIdentifiers,
        columns=[GenderCurrent, PersonBirthDate, ExpectedPersonBirthDate],
        row_df=df_issues,
    )

def test_validate():
    # Create some sample data such that some values pass the validation and some fail.
    child_identifiers = pd.DataFrame(
        [
            {  # 0 - Pass - Not unborn
                "LAchildID": "child1",
                "PersonBirthDate": "26/05/2000",
                "ExpectedPersonBirthDate": "26/05/2000",
                "GenderCurrent": "1",
            },
            {  # 1 - Pass - Not unborn
                "LAchildID": "child2",
                "PersonBirthDate": "26/05/2000",
                "ExpectedPersonBirthDate": pd.NA,
                "GenderCurrent": "2",
            },
            {  # 2 - Pass - Unborn with Gender = 0
                "LAchildID": "child3",
                "PersonBirthDate": pd.NA,
                "ExpectedPersonBirthDate": "26/05/1999",
                "GenderCurrent": "0",
            },
            {  # 3 - Pass - Not unborn or born! (Not relevant to this rule)
                "LAchildID": "child3",
                "PersonBirthDate": pd.NA,
                "ExpectedPersonBirthDate": pd.NA,
                "GenderCurrent": "2",
            },
            {  # 4 - Fail - Unborn with Gender = 2
                "LAchildID": "child4",
                "PersonBirthDate": pd.NA,
                "ExpectedPersonBirthDate": "25/05/2000",
                "GenderCurrent": "2",
            },
            {  # 5 - Fail - Unborn with Gender = 1
                "LAchildID": "child4",
                "PersonBirthDate": pd.NA,
                "ExpectedPersonBirthDate": "25/05/2000",
                "GenderCurrent": "1",
            },
        ]
    )
    # if rule requires columns containing date values, convert those columns to datetime objects first. Do it here in the test_validate function, not above.
    child_identifiers[PersonBirthDate] = pd.to_datetime(
        child_identifiers[PersonBirthDate], format="%d/%m/%Y", errors="coerce"
    )
    child_identifiers[ExpectedPersonBirthDate] = pd.to_datetime(
        child_identifiers[ExpectedPersonBirthDate], format="%d/%m/%Y", errors="coerce"
    )

    # Run rule function passing in our sample data
    result = run_rule(validate, {ChildIdentifiers: child_identifiers})

    # Use .type1_issues to check for the result of .push_type1_issues() which you used above.
    issues = result.type1_issues

    # get table name and check it. Replace ChildProtectionPlans with the name of your table.
    issue_table = issues.table
    assert issue_table == ChildIdentifiers

    # check that the right columns were returned. Replace CPPstartDate and CPPendDate with a list of your columns.
    issue_columns = issues.columns
    assert issue_columns == [GenderCurrent, PersonBirthDate, ExpectedPersonBirthDate]

    # check that the location linking dataframe was formed properly.
    issue_rows = issues.row_df
    # replace 2 with the number of failing points you expect from the sample data.
    assert len(issue_rows) == 2
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
                "ERROR_ID": (
                    "child4",
                    "1",
                    pd.to_datetime("25/05/2000", format="%d/%m/%Y", errors="coerce"),
                ),
                "ROW_ID": [5],
            },
            {
                "ERROR_ID": (
                    "child4",
                    "2",
                    pd.to_datetime("25/05/2000", format="%d/%m/%Y", errors="coerce"),
                ),
                "ROW_ID": [4],
            },
        ]
    )
    assert issue_rows.equals(expected_df)

    # Check that the rule definition is what you wrote in the context above.

    # replace 8925 with the rule code and put the appropriate message in its place too.
    assert result.definition.code == 8750
    assert result.definition.message == "Gender must equal 0 for an unborn child"
