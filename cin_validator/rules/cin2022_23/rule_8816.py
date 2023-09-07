from typing import Mapping

import pandas as pd

from cin_validator.rule_engine import CINTable, RuleContext, rule_definition
from cin_validator.test_engine import run_rule

# Get tables and columns of interest from the CINTable object defined in rule_engine/__api.py

CINdetails = CINTable.CINdetails
LAchildID = CINdetails.LAchildID
CINdetailsID = CINdetails.CINdetailsID
ReferralNFA = CINdetails.ReferralNFA
CINreferralDate = CINdetails.CINreferralDate
CINclosureDate = CINdetails.CINclosureDate


# define characteristics of rule
@rule_definition(
    # write the rule code here
    code="8816",
    # replace CINdetails with the value in the module column of the excel sheet corresponding to this rule .
    # Note that even if multiple tables are involved, one table will be named in the module column.
    module=CINTable.CINdetails,
    # replace the message with the corresponding value for this rule, gotten from the excel sheet.
    message="An open CIN episode is shown and case is not a referral with no further action, but it is not the latest episode.",
    # The column names tend to be the words within the < > signs in the github issue description.
    affected_fields=[
        CINreferralDate,
        ReferralNFA,
    ],
)
def validate(
    data_container: Mapping[CINTable, pd.DataFrame], rule_context: RuleContext
):
    # PREPARING DATA

    df_cin = data_container[CINdetails].copy()

    # Before you begin, rename the index so that the initial row positions can be kept intact.
    df_cin.index.name = "ROW_ID"

    # If there is a <CINdetails> module present for the child where:
    # <CINclosureDate> (N00102) is missing; and
    # <ReferralNFA> (N00112) = false or 0
    # then the <CINreferralDate> (N00100) for this module must be the latest of all Referral Dates for that child.

    # removing nans prevents ValueError: attempt to get argmax of an empty sequence, when no CINreferralDate is present
    df_cin = df_cin[df_cin[CINreferralDate].notna()]

    # find out the latest referral date (and its index position) for each child and attach it to all the rows for that child.
    df_cin["latest_referral"] = df_cin.groupby(LAchildID)[CINreferralDate].transform(
        "max"
    )
    df_cin["latest_referral_ind"] = df_cin.groupby(LAchildID)[
        CINreferralDate
    ].transform("idxmax")

    # resetting the index makes ROW_ID into a column to keep it intact. The logic can now begin.
    df_cin.reset_index(inplace=True)

    # filter the rows of interest

    df_cin = df_cin[
        (df_cin[CINclosureDate].isna()) & (df_cin[ReferralNFA].isin(["false", "0"]))
    ]
    # check those that do not meet the requirements
    condition = df_cin["ROW_ID"] != df_cin["latest_referral_ind"]
    df_cin = df_cin[condition]

    df_cin["ERROR_ID"] = tuple(
        zip(df_cin[LAchildID], df_cin[CINdetailsID], df_cin[CINreferralDate])
    )

    df_cin_issues = (
        df_cin.groupby("ERROR_ID", group_keys=False)["ROW_ID"].apply(list).reset_index()
    )
    # the locations of the latest_referral date should also be flagged
    df_cin_issues_cause = (
        df_cin.groupby("ERROR_ID", group_keys=False)["latest_referral_ind"]
        .apply(list)
        .reset_index()
    )
    # rename column to fit expected structure of a df_issues dataframe
    df_cin_issues_cause.rename(columns={"latest_referral_ind": "ROW_ID"}, inplace=True)

    # Ensure that you maintain the ROW_ID, and ERROR_ID column names which are shown above. They are keywords in this project.
    rule_context.push_type_3(
        table=CINdetails,
        columns=[CINreferralDate],
        row_df=df_cin_issues,
    )
    rule_context.push_type_3(
        table=CINdetails,
        columns=[CINreferralDate],
        row_df=df_cin_issues_cause,
    )


def test_validate():
    # Create some sample data such that some values pass the validation and some fail.

    sample_cin = pd.DataFrame(
        [
            {
                "LAchildID": "child1",
                "CINclosureDate": pd.NA,
                "CINreferralDate": "26/10/2000",  # 0 fail: not latest referral in child1
                "CINdetailsID": "cinID1",
                "ReferralNFA": "0",
            },
            {
                "LAchildID": "child1",
                "CINclosureDate": "26/05/2001",
                "CINreferralDate": "26/10/2001",  # 1 ignore: CINclosureDate notna
                "CINdetailsID": "cinID2",
                "ReferralNFA": "0",
            },
            {
                "LAchildID": "child1",
                "CINclosureDate": pd.NA,
                "CINreferralDate": "26/10/1999",  # 2 fail: not latest referral in child1
                "CINdetailsID": "cinID3",
                "ReferralNFA": "0",
            },
            {
                "LAchildID": "child1",
                "CINclosureDate": pd.NA,
                "CINreferralDate": "26/10/2002",  # 3 pass: latest referral in child1
                "CINdetailsID": "cinID4",
                "ReferralNFA": "0",
            },
            # child2
            {
                "LAchildID": "child2",
                "CINclosureDate": pd.NA,
                "CINreferralDate": "26/10/1999",  # 4 pass: latest referral in child2
                "CINdetailsID": "cinID3",
                "ReferralNFA": "0",
            },
            {
                "LAchildID": "child2",
                "CINclosureDate": pd.NA,
                "CINreferralDate": "26/10/1990",  # 5 fail: not latest referral in child2
                "CINdetailsID": "cinID3",
                "ReferralNFA": "0",
            },
            {
                "LAchildID": "child4",
                "CINclosureDate": pd.NA,
                "CINreferralDate": pd.NA,
                "CINdetailsID": "cinID3",
                "ReferralNFA": "0",
            },
            {
                "LAchildID": "child4",
                "CINclosureDate": pd.NA,
                "CINreferralDate": pd.NA,
                "CINdetailsID": "cinID4",
                "ReferralNFA": "0",
            },
            {
                "LAchildID": "child5",
                "CINreferralDate": "26/10/1999",
                "CINdetailsID": "cinID4",
                "ReferralNFA": "0",
            },
            {
                "LAchildID": "child5",  # 9, Fail, shared CINreferralDate
                "CINdetailsID": "cinID5",
                "CINreferralDate": "26/10/1999",
                "ReferralNFA": "0",
            },
        ]
    )

    # If rule requires columns containing date values, convert those columns to datetime objects first. Do it here in the test_validate function, not above.
    sample_cin[CINclosureDate] = pd.to_datetime(
        sample_cin[CINclosureDate], format="%d/%m/%Y", errors="coerce"
    )
    sample_cin[CINreferralDate] = pd.to_datetime(
        sample_cin[CINreferralDate], format="%d/%m/%Y", errors="coerce"
    )

    # Run the rule function, passing in our sample data.
    result = run_rule(
        validate,
        {
            CINdetails: sample_cin,
        },
    )

    # Use .type3_issues to check for the result of .push_type2_issues() which you used above.
    issues_list = result.type3_issues
    assert len(issues_list) == 2
    # the function returns a list on NamedTuples where each NamedTuple contains (table, column_list, df_issues)
    # pick any table and check it's values.
    issues = issues_list[0]

    issue_table = issues.table
    assert issue_table == CINdetails

    issue_columns = issues.columns
    assert issue_columns == [CINreferralDate]

    # check that the location linking dataframe was formed properly.
    issue_rows = issues.row_df
    # replace 3 with the number of failing points you expect from the sample data.
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
                    "child1",
                    "cinID1",
                    pd.to_datetime("26/10/2000", format="%d/%m/%Y"),
                ),
                "ROW_ID": [0],
            },
            {
                "ERROR_ID": (
                    "child1",
                    "cinID3",
                    pd.to_datetime("26/10/1999", format="%d/%m/%Y"),
                ),
                "ROW_ID": [2],
            },
            {
                "ERROR_ID": (
                    "child2",
                    "cinID3",
                    pd.to_datetime("26/10/1990", format="%d/%m/%Y"),
                ),
                "ROW_ID": [5],
            },
            {
                "ERROR_ID": (
                    "child5",
                    "cinID5",
                    pd.to_datetime("26/10/1999", format="%d/%m/%Y"),
                ),
                "ROW_ID": [9],
            },
        ]
    )
    assert issue_rows.equals(expected_df)

    # Check that the rule definition is what you wrote in the context above.

    # replace '8816' with the rule code and put the appropriate message in its place too.
    assert result.definition.code == "8816"
    assert (
        result.definition.message
        == "An open CIN episode is shown and case is not a referral with no further action, but it is not the latest episode."
    )
