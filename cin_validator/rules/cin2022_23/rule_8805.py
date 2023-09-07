from typing import Mapping

import pandas as pd

from cin_validator.rule_engine import CINTable, RuleContext, rule_definition
from cin_validator.test_engine import run_rule

# Get tables and columns of interest from the CINTable object defined in rule_engine/__api.py

CINDetails = CINTable.CINdetails
CINclosureDate = CINDetails.CINclosureDate
ReasonForClosure = CINDetails.ReasonForClosure
LAchildID = CINDetails.LAchildID


# define characteristics of rule
@rule_definition(
    # write the rule code here, in place of '8805'
    code="8805",
    module=CINTable.CINdetails,
    # replace the message with the corresponding value for this rule, gotten from the excel sheet.
    message="A CIN case cannot have a CIN closure date without a Reason for Closure",
    # The column names tend to be the words within the < > signs in the github issue description.
    affected_fields=[CINclosureDate, ReasonForClosure],
)
def validate(
    data_container: Mapping[CINTable, pd.DataFrame], rule_context: RuleContext
):
    # PREPARING DATA

    df = data_container[CINDetails]
    # Before you begin, rename the index so that the initial row positions can be kept intact.
    df.index.name = "ROW_ID"

    # lOGIC
    # Implement rule logic as described by the Github issue.
    # Put the description as a comment above the implementation as shown.

    # If <CINclosureDate> (N00102) is present then <ReasonForClosure> (N00103) must also be present
    # return rows where CINClosureDate is present but ReasonForClosure is not.
    # If CINclosureDate is not null and ReasonForClosure is null
    condition = df[CINclosureDate].notna() & df[ReasonForClosure].isna()

    # get all the data that fits the failing condition. Reset the index so that ROW_ID now becomes a column of df
    df_issues = df[condition].reset_index()

    # SUBMIT ERRORS

    link_id = tuple(
        zip(
            df_issues[LAchildID], df_issues[CINclosureDate], df_issues[ReasonForClosure]
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
        table=CINDetails, columns=[CINclosureDate, ReasonForClosure], row_df=df_issues
    )


def test_validate():
    # Create some sample data such that some values pass the validation and some fail.
    fake_date_frame = pd.DataFrame(
        [
            {
                "LAchildID": "child1",
                "CINclosureDate": "26/05/2000",
                "ReasonForClosure": "26/05/2000",
            },
            {
                "LAchildID": "child2",
                "CINclosureDate": pd.NA,
                "ReasonForClosure": "26/05/2000",
            },
            {
                "LAchildID": "child3",
                "CINclosureDate": "26/05/1999",
                "ReasonForClosure": "26/05/2000",
            },
            {
                "LAchildID": "child3",
                "CINclosureDate": "26/05/2000",
                "ReasonForClosure": pd.NA,
            },  # fail because CINClosureDate is populated and ReasonForClosure isn't
            {
                "LAchildID": "child4",
                "CINclosureDate": "25/05/2000",
                "ReasonForClosure": pd.NA,
            },  # fail because CINClosureDate is populated and ReasonForClosure isn't
            {
                "LAchildID": "child5",
                "CINclosureDate": pd.NA,
                "ReasonForClosure": pd.NA,
            },
        ]
    )

    # Date values not checked so no datetime conversion required

    # Run rule function passing in our sample data
    result = run_rule(validate, {CINDetails: fake_date_frame})

    # Use .type1_issues to check for the result of .push_type1_issues() which you used above.
    issues = result.type1_issues

    # get table name and check it.
    issue_table = issues.table
    assert issue_table == CINDetails

    # check that the right columns were returned.
    issue_columns = issues.columns
    assert issue_columns == [CINclosureDate, ReasonForClosure]

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
                    "26/05/2000",
                    pd.NA,
                ),
                "ROW_ID": [3],
            },
            {
                "ERROR_ID": (
                    "child4",
                    "25/05/2000",
                    pd.NA,
                ),
                "ROW_ID": [4],
            },
        ]
    )
    assert issue_rows.equals(expected_df)

    # Check that the rule definition is what you wrote in the context above.

    # replace '8805' with the rule code and put the appropriate message in its place too.
    assert result.definition.code == "8805"
    assert (
        result.definition.message
        == "A CIN case cannot have a CIN closure date without a Reason for Closure"
    )
