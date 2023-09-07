from typing import Mapping

import pandas as pd

from cin_validator.rule_engine import CINTable, RuleContext, rule_definition
from cin_validator.test_engine import run_rule

# Get tables and columns of interest from the CINTable object defined in rule_engine/__api.py

CINdetails = CINTable.CINdetails
LAchildID = CINdetails.LAchildID
CINreferralDate = CINdetails.CINreferralDate
ReferralSource = CINdetails.ReferralSource


# define characteristics of rule
@rule_definition(
    # write the rule code here, in place of '8866'
    code="8866",
    # replace CINdetails with the value in the module column of the excel sheet corresponding to this rule .
    module=CINTable.CINdetails,
    # replace the message with the corresponding value for this rule, gotten from the excel sheet.
    message="Source of Referral is missing or an invalid code",
    # The column names tend to be the words within the < > signs in the github issue description.
    affected_fields=[
        CINreferralDate,
        ReferralSource,
    ],
)
def validate(
    data_container: Mapping[CINTable, pd.DataFrame], rule_context: RuleContext
):
    # Replace CINdetails with the name of the table you need.
    df = data_container[CINdetails]
    # Before you begin, rename the index so that the initial row positions can be kept intact.
    df.index.name = "ROW_ID"
    df.reset_index(inplace=True)

    valid_referrals = [
        "1A",
        "1B",
        "1C",
        "1D",
        "2A",
        "2B",
        "3A",
        "3B",
        "3C",
        "3D",
        "3E",
        "3F",
        "4",
        "5A",
        "5B",
        "5C",
        "5D",
        "6",
        "7",
        "8",
        "9",
        "10",
    ]

    # If <CinReferralDate> (N00100) is on or after 1 April 2013 then <ReferralSource> (N00152) must be present and must be a valid code
    condition = (
        df[CINreferralDate] >= pd.to_datetime("01/04/2013", format="%d/%m/%Y")
    ) & ((df[ReferralSource].isna()) | (~df[ReferralSource].isin(valid_referrals)))
    # get all the data that fits the failing condition. Reset the index so that ROW_ID now becomes a column of df
    df_issues = df[condition].reset_index()

    # SUBMIT ERRORS
    # Generate a unique ID for each instance of an error.

    # Replace CPPstartDate and CPPendDate below with the columns concerned in your rule.
    link_id = tuple(zip(df_issues[LAchildID], df_issues[CINreferralDate]))
    df_issues["ERROR_ID"] = link_id
    df_issues = (
        df_issues.groupby("ERROR_ID", group_keys=False)["ROW_ID"]
        .apply(list)
        .reset_index()
    )
    # Ensure that you do not change the ROW_ID, and ERROR_ID column names which are shown above. They are keywords in this project.
    rule_context.push_type_1(
        table=CINdetails, columns=[CINreferralDate, ReferralSource], row_df=df_issues
    )


def test_validate():
    # Create some sample data such that some values pass the validation and some fail.

    #  Fails rows 0, 1, and 3
    sample_cin = pd.DataFrame(
        [
            {
                "LAchildID": "ID1",  # Fail
                "CINreferralDate": "01/01/2020",
                "ReferralSource": "1Z",
            },
            {
                "LAchildID": "ID2",  # Fail
                "CINreferralDate": "01/01/2020",
                "ReferralSource": pd.NA,
            },
            {
                "LAchildID": "ID3",
                "CINreferralDate": "01/01/2000",
                "ReferralSource": "1A",
            },
            {
                "LAchildID": "ID4",
                "CINreferralDate": "01/01/2012",  # ignore
                "ReferralSource": "pd.NA",
            },
            {
                "LAchildID": "ID4",
                "CINreferralDate": "01/01/2020",
                "ReferralSource": "1C",
            },
        ]
    )
    sample_cin["CINreferralDate"] = pd.to_datetime(
        sample_cin["CINreferralDate"], format="%d/%m/%Y", errors="coerce"
    )

    # Run rule function passing in our sample data
    result = run_rule(validate, {CINdetails: sample_cin})

    # The result contains a NamedTuple of issues encountered
    issues = result.type1_issues

    issue_table = issues.table
    assert issue_table == CINdetails

    issue_columns = issues.columns
    assert issue_columns == [CINreferralDate, ReferralSource]

    # check that the location linking dataframe was formed properly.
    issue_rows = issues.row_df

    # replace 2 with the number of failing points you expect from the sample data.
    assert len(issue_rows) == 2
    # replace the table and column name as done earlier.
    # The last numbers represent the index values where you expect the sample data to fail the validation check.
    # check that the failing locations are contained in a DataFrame having the appropriate columns. These lines do not change.
    assert isinstance(issue_rows, pd.DataFrame)
    assert issue_rows.columns.to_list() == ["ERROR_ID", "ROW_ID"]

    # The ROW ID values represent the index positions where you expect the sample data to fail the validation check.
    expected_df = pd.DataFrame(
        [
            {
                "ERROR_ID": (
                    "ID1",
                    pd.to_datetime("01/01/2020", format="%d/%m/%Y", errors="coerce"),
                ),
                "ROW_ID": [0],
            },
            {
                "ERROR_ID": (
                    "ID2",
                    pd.to_datetime("01/01/2020", format="%d/%m/%Y", errors="coerce"),
                ),
                "ROW_ID": [1],
            },
        ]
    )
    assert issue_rows.equals(expected_df)

    # Check that the rule definition is what you wrote in the context above.

    assert result.definition.code == "8866"
    assert (
        result.definition.message == "Source of Referral is missing or an invalid code"
    )
