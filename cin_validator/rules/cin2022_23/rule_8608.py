from typing import Mapping

import pandas as pd

from cin_validator.rule_engine import CINTable, RuleContext, rule_definition
from cin_validator.test_engine import run_rule

# Get tables and columns of interest from the CINTable object defined in rule_engine/__api.py

Assessments = CINTable.Assessments
LAchildID = Assessments.LAchildID
AssessmentActualStartDate = Assessments.AssessmentActualStartDate
AssessmentAuthorisationDate = Assessments.AssessmentAuthorisationDate


# define characteristics of rule
@rule_definition(
    # write the rule code here, in place of 8500
    code=8608,
    # replace ChildIdentifiers with the value in the module column of the excel sheet corresponding to this rule .
    module=CINTable.Assessments,
    # replace the message with the corresponding value for this rule, gotten from the excel sheet.
    message="Assessment Start Date cannot be later than its End Date",
    # The column names tend to be the words within the < > signs in the github issue description.
    affected_fields=[AssessmentActualStartDate, AssessmentAuthorisationDate],
)
def validate(
    data_container: Mapping[CINTable, pd.DataFrame], rule_context: RuleContext
):
    # PREPARING DATA

    # Replace ChildIdentifiers with the name of the table you need.
    df = data_container[Assessments]
    # Before you begin, rename the index so that the initial row positions can be kept intact.
    df.index.name = "ROW_ID"

    # lOGIC
    # Implement rule logic as described by the Github issue.
    # Put the description as a comment above the implementation as shown.

    # If present <AssessmentAuthorisationDate> (N00160) must be on or after the <AssessmentActualStartDate> (N00159)
    condition = df[AssessmentActualStartDate] > df[AssessmentAuthorisationDate]
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
    # Ensure that you do not change the ROW_ID, and ERROR_ID column names which are shown above. They are keywords in this project.
    rule_context.push_type_1(
        table=Assessments,
        columns=[AssessmentActualStartDate, AssessmentAuthorisationDate],
        row_df=df_issues,
    )


def test_validate():
    # Create some sample data such that some values pass the validation and some fail.
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
    # if rule requires columns containing date values, convert those columns to datetime objects first. Do it here in the test_validate function, not above.
    assessments[AssessmentActualStartDate] = pd.to_datetime(
        assessments[AssessmentActualStartDate], format="%d/%m/%Y", errors="coerce"
    )
    assessments[AssessmentAuthorisationDate] = pd.to_datetime(
        assessments[AssessmentAuthorisationDate], format="%d/%m/%Y", errors="coerce"
    )

    # Run rule function passing in our sample data
    result = run_rule(validate, {Assessments: assessments})

    # Use .type1_issues to check for the result of .push_type1_issues() which you used above.
    issues = result.type1_issues

    # get table name and check it. Replace ChildProtectionPlans with the name of your table.
    issue_table = issues.table
    assert issue_table == Assessments

    # check that the right columns were returned. Replace CPPstartDate and CPPendDate with a list of your columns.
    issue_columns = issues.columns
    assert issue_columns == [AssessmentActualStartDate, AssessmentAuthorisationDate]

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

    # Check that the rule definition is what you wrote in the context above.

    # replace 8608 with the rule code and put the appropriate message in its place too.
    assert result.definition.code == 8608
    assert (
        result.definition.message
        == "Assessment Start Date cannot be later than its End Date"
    )
