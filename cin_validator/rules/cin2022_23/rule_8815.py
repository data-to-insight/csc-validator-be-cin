from typing import Mapping

import pandas as pd

from cin_validator.rule_engine import CINTable, RuleContext, rule_definition
from cin_validator.test_engine import run_rule

# Get tables and columns of interest from the CINTable object defined in rule_engine/__api.py

CINdetails = CINTable.CINdetails
LAchildID = CINdetails.LAchildID
CINdetailsID = CINdetails.CINdetailsID
CINclosureDate = CINdetails.CINclosureDate
ReferralNFA = CINdetails.ReferralNFA

Header = CINTable.Header
ReferenceDate = Header.ReferenceDate


# define characteristics of rule
@rule_definition(
    # write the rule code here
    code="8815",
    # replace CINdetails with the value in the module column of the excel sheet corresponding to this rule .
    # Note that even if multiple tables are involved, one table will be named in the module column.
    module=CINTable.CINdetails,
    # replace the message with the corresponding value for this rule, gotten from the excel sheet.
    message="More than one open CIN Details episode (a module with no CIN Closure Date) has been provided for this child and case is not a referral with no further action.",
    # The column names tend to be the words within the < > signs in the github issue description.
    affected_fields=[
        ReferralNFA,
        CINclosureDate,
    ],
)
def validate(
    data_container: Mapping[CINTable, pd.DataFrame], rule_context: RuleContext
):
    # PREPARING DATA

    df = data_container[CINdetails]
    # Rename and reset index
    df.index.name = "ROW_ID"
    df.reset_index(inplace=True)

    # If more than one <CINdetails> module is present for the same child, then there must only be one where:
    # <CINclosureDate> (N00102) is missing; and
    # <ReferralNFA> (N00112) = false or 0

    # Notes: a) <CINclosureDate> (N00102) can be present on all modules,
    # b) there may be more than one module with no <CINclosureDate> where <ReferralNFA> (N00112) = true/1.
    falsezero = ["false", "0"]
    df_check = df.copy()
    df_check = df_check[
        df_check[CINclosureDate].isna() & (df_check[ReferralNFA].isin(falsezero))
    ]

    # Convert NAs to 1 and count by child
    df_check[CINclosureDate].fillna(1, inplace=True)

    df_check = df_check.groupby([LAchildID])[CINclosureDate].count().reset_index()

    # Find where there is more than 1 open end date
    # Note, this is done differently to other similar rules, because they compare within one module, and this compares all open modules of the same type.
    fail_list = df_check[df_check[CINclosureDate] > 1]["LAchildID"].tolist()

    # Some of these lines are technically redundant and could be compressed but I've kept them to keep the structure of the rules
    df_bad = df[(df[LAchildID].isin(fail_list)) & df[CINclosureDate].isna()]

    issue_ids = tuple(zip(df_bad[LAchildID], df_bad[CINdetailsID]))

    df["ERROR_ID"] = tuple(zip(df[LAchildID], df[CINdetailsID]))
    df_issues = df[df["ERROR_ID"].isin(issue_ids)]

    df_issues = (
        df_issues.groupby("ERROR_ID", group_keys=False)["ROW_ID"]
        .apply(list)
        .reset_index()
    )

    rule_context.push_type_3(
        table=CINdetails, columns=[CINclosureDate], row_df=df_issues
    )


def test_validate():
    # Create some sample data such that some values pass the validation and some fail.
    sample_header = pd.DataFrame(
        [{ReferenceDate: "31/03/2001"}]  # the census start date here will be 01/04/2000
    )

    sample_cin = pd.DataFrame(
        [
            {
                "LAchildID": "child1",  # 0 Pass
                "CINreferralDate": "26/05/2000",
                "CINclosureDate": pd.NA,
                "CINdetailsID": "cinID1",
                "ReferralNFA": "true",
            },
            {
                "LAchildID": "child1",  # Pass
                "CINreferralDate": "26/08/2000",
                "CINclosureDate": pd.NA,
                "CINdetailsID": "cinID12",
                "ReferralNFA": "0",
            },
            {
                "LAchildID": "child2",  # 2 Pass
                "CINreferralDate": "26/05/2000",
                "CINclosureDate": "25/10/2000",
                "CINdetailsID": "cinID2",
                "ReferralNFA": "true",
            },
            {
                "LAchildID": "child2",  # 3 Pass
                "CINreferralDate": "26/10/2000",
                "CINclosureDate": "26/12/2000",
                "CINdetailsID": "cinID22",
                "ReferralNFA": "true",
            },
            {
                "LAchildID": "child3",  # 4 Fail
                "CINreferralDate": "26/05/2000",
                "CINclosureDate": pd.NA,
                "CINdetailsID": "cinID3",
                "ReferralNFA": "0",
            },
            {
                "LAchildID": "child3",  # 5 Fail
                "CINreferralDate": "26/08/2000",
                "CINclosureDate": pd.NA,
                "CINdetailsID": "cinID32",
                "ReferralNFA": "0",
            },
            {
                "LAchildID": "child4",  # 6 Fail
                "CINreferralDate": "26/10/2000",
                "CINclosureDate": pd.NA,
                "CINdetailsID": "cinID4",
                "ReferralNFA": "false",
            },
            {
                "LAchildID": "child4",  # 7 Fail
                "CINreferralDate": "31/03/2001",
                "CINclosureDate": pd.NA,
                "CINdetailsID": "cinID42",
                "ReferralNFA": "0",
            },
        ]
    )

    # If rule requires columns containing date values, convert those columns to datetime objects first. Do it here in the test_validate function, not above.
    sample_cin["CINreferralDate"] = pd.to_datetime(
        sample_cin["CINreferralDate"], format="%d/%m/%Y", errors="coerce"
    )
    sample_cin[CINclosureDate] = pd.to_datetime(
        sample_cin[CINclosureDate], format="%d/%m/%Y", errors="coerce"
    )
    sample_header[ReferenceDate] = pd.to_datetime(
        sample_header[ReferenceDate], format="%d/%m/%Y", errors="coerce"
    )

    # Run the rule function, passing in our sample data.
    result = run_rule(
        validate,
        {
            CINdetails: sample_cin,
            Header: sample_header,
        },
    )

    # Use .type3_issues to check for the result of .push_type3_issues() which you used above.
    issues_list = result.type3_issues
    assert len(issues_list) == 1
    # the function returns a list on NamedTuples where each NamedTuple contains (table, column_list, df_issues)
    # pick any table and check it's values. the tuple in location 0 will contain the CINdetails columns because that's the second thing pushed above.
    issues = issues_list[0]

    # get table name and check it. Replace CINdetails with the name of your table.
    issue_table = issues.table
    assert issue_table == CINdetails

    # check that the right columns were returned. Replace CINclosureDate with a list of your columns.
    issue_columns = issues.columns
    assert issue_columns == [CINclosureDate]

    # check that the location linking dataframe was formed properly.
    issue_rows = issues.row_df
    assert len(issue_rows) == 4

    # check that the failing locations are contained in a DataFrame having the appropriate columns. These lines do not change.
    assert isinstance(issue_rows, pd.DataFrame)
    assert issue_rows.columns.to_list() == ["ERROR_ID", "ROW_ID"]

    # Create the dataframe which you expect, based on the fake data you created. It should have two columns.
    # - The first column is ERROR_ID which contains the unique combination that identifies each error instance, which you decided on, in your zip, earlier.
    # - The second column in ROW_ID which contains a list of index positions that belong to each error instance.

    # The ROW ID values represent the index positions where you expect the sample data to fail the validation check.
    expected_df = pd.DataFrame(
        [
            {
                "ERROR_ID": (
                    "child3",
                    "cinID3",
                ),
                "ROW_ID": [4],
            },
            {
                "ERROR_ID": (
                    "child3",
                    "cinID32",
                ),
                "ROW_ID": [5],
            },
            {
                "ERROR_ID": (
                    "child4",
                    "cinID4",
                ),
                "ROW_ID": [6],
            },
            {
                "ERROR_ID": (
                    "child4",
                    "cinID42",
                ),
                "ROW_ID": [7],
            },
        ]
    )
    assert issue_rows.equals(expected_df)

    # Check that the rule definition is what you wrote in the context above.

    # replace '8815' with the rule code and put the appropriate message in its place too.
    assert result.definition.code == "8815"
    assert (
        result.definition.message
        == "More than one open CIN Details episode (a module with no CIN Closure Date) has been provided for this child and case is not a referral with no further action."
    )
