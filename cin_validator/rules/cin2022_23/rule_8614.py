from typing import Mapping

import pandas as pd

from cin_validator.rule_engine import CINTable, RuleContext, rule_definition
from cin_validator.test_engine import run_rule

# Get tables and columns of interest from the CINTable object defined in rule_engine/__api.py

Assessments = CINTable.Assessments
AssessmentAuthorisationDate = Assessments.AssessmentAuthorisationDate
AssessmentFactors = Assessments.AssessmentFactors
LAchildID = Assessments.LAchildID

# define characteristics of rule
@rule_definition(
    # write the rule code here, in place of 8500
    code=8614,
    # replace ChildIdentifiers with the value in the module column of the excel sheet corresponding to this rule .
    module=CINTable.Assessments,
    # replace the message with the corresponding value for this rule, gotten from the excel sheet.
    message="Parental or child factors at assessment should only be present for a completed assessment.",
    # The column names tend to be the words within the < > signs in the github issue description.
    affected_fields=[AssessmentAuthorisationDate, AssessmentFactors],
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

    # If <AssessmentAuthorisationDate> (N00160) is missing,
    # <AssessmentFactors> (N00181) within the same module must also be missing
    # fails if AssessmentAuthoriationDate is null and AssessmentFactors  is not null
    condition = df[AssessmentAuthorisationDate].isna() & df[AssessmentFactors].notna()

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
    # Ensure that you do not change the ROW_ID, and ERROR_ID column names which are shown above. They are keywords in this project.
    rule_context.push_type_1(
        table=Assessments,
        columns=[AssessmentAuthorisationDate, AssessmentFactors],
        row_df=df_issues,
    )


def test_validate():
    # Create some sample data such that some values pass the validation and some fail.
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
    # Date check not required
    # if rule requires columns containing date values, convert those columns to datetime objects first. Do it here in the test_validate function, not above.
    # child_protection_plans[CPPstartDate] = pd.to_datetime(
    #    child_protection_plans[CPPstartDate], format="%d/%m/%Y", errors="coerce"
    # )
    # child_protection_plans[CPPendDate] = pd.to_datetime(
    #    child_protection_plans[CPPendDate], format="%d/%m/%Y", errors="coerce"
    # )

    # Run rule function passing in our sample data
    result = run_rule(validate, {Assessments: fake_data})

    # Use .type1_issues to check for the result of .push_type1_issues() which you used above.
    issues = result.type1_issues

    # get table name and check it. Replace ChildProtectionPlans with the name of your table.
    issue_table = issues.table
    assert issue_table == Assessments

    # check that the right columns were returned. Replace CPPstartDate and CPPendDate with a list of your columns.
    issue_columns = issues.columns
    assert issue_columns == [AssessmentAuthorisationDate, AssessmentFactors]

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

    # Check that the rule definition is what you wrote in the context above.

    # replace 8925 with the rule code and put the appropriate message in its place too.
    assert result.definition.code == 8614
    assert (
        result.definition.message
        == "Parental or child factors at assessment should only be present for a completed assessment."
    )
