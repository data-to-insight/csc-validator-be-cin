from typing import Mapping

import pandas as pd

from cin_validator.rule_engine import CINTable, RuleContext, rule_definition
from cin_validator.test_engine import run_rule
from cin_validator.utils import make_census_period

# Get tables and columns of interest from the CINTable object defined in rule_engine/__api.py


CINdetails = CINTable.CINdetails

ReferralNFA = CINdetails.ReferralNFA
LAchildID = CINdetails.LAchildID
CINdetailsID = CINdetails.CINdetailsID
CINreferralDate = CINdetails.CINreferralDate

# Reference date in header is needed to define the period of census.
Header = CINTable.Header
ReferenceDate = Header.ReferenceDate

# define characteristics of rule
@rule_definition(
    # write the rule code here, in place of 8569
    code=8569,
    module=CINTable.CINdetails,
    # replace the message with the corresponding value for this rule, gotten from the excel sheet.
    message="A case with referral date before one working day prior to the collection start date must not be flagged as a no further action case",
    # The column names tend to be the words within the < > signs in the github issue description.
    affected_fields=[
        ReferralNFA,
    ],
)
def validate(
    data_container: Mapping[CINTable, pd.DataFrame], rule_context: RuleContext
):
    df_cin = data_container[CINdetails].copy()
    df_cin.index.name = "ROW_ID"

    df_cin.reset_index(inplace=True)

    # get collection period
    header = data_container[Header]
    ref_date_series = header[ReferenceDate]
    collection_start, collection_end = make_census_period(ref_date_series)

    # If <CINreferralDate> (N00100) is before [Start_of_Census_Year] minus 1 working day, <ReferralNFA> (N00112) must be false

    df_cin_issues = df_cin[
        df_cin[CINreferralDate] < (collection_start - pd.tseries.offsets.BDay(1))
    ]

    df_cin_issues = df_cin_issues[
        ~df_cin_issues[ReferralNFA].isin(["false", "0"])
    ].reset_index()

    df_cin_issues["ERROR_ID"] = tuple(
        zip(
            df_cin_issues[LAchildID],
            df_cin_issues[CINdetailsID],
            df_cin_issues[CINreferralDate],
        )
    )

    df_issues = (
        df_cin_issues.groupby("ERROR_ID", group_keys=False)["ROW_ID"]
        .apply(list)
        .reset_index()
    )

    rule_context.push_type_2(
        table=CINdetails, columns=[CINreferralDate], row_df=df_issues
    )


def test_validate():
    # Create some sample data such that some values pass the validation and some fail.

    sample_cin_details = pd.DataFrame(
        [
            {
                "LAchildID": "child1",
                "CINreferralDate": "26/10/1880",
                "ReferralNFA": "true",
                "CINdetailsID": "cinID1",
            },
            {
                "LAchildID": "child2",
                "CINreferralDate": "26/10/2001",
                "ReferralNFA": "true",
                "CINdetailsID": "cinID2",
            },
            {
                "LAchildID": "child3",
                "CINreferralDate": "26/10/1880",
                "ReferralNFA": "false",
                "CINdetailsID": "cinID3",
            },
            {
                "LAchildID": "child4",
                "CINreferralDate": "26/10/1999",
                "ReferralNFA": "false",
                "CINdetailsID": "cinID4",
            },
            {
                "LAchildID": "child5",
                "CINreferralDate": pd.NA,
                "ReferralNFA": "false",
                "CINdetailsID": "cinID5",
            },
        ]
    )
    # if rule requires columns containing date values, convert those columns to datetime objects first. Do it here in the test_validate function, not above.

    sample_cin_details["CINreferralDate"] = pd.to_datetime(
        sample_cin_details["CINreferralDate"], format="%d/%m/%Y", errors="coerce"
    )
    sample_header = pd.DataFrame(
        [{ReferenceDate: "31/03/2000"}]  # the census start date here will be 01/04/2000
    )

    # Run the rule function, passing in our sample data.
    result = run_rule(
        validate,
        {
            CINdetails: sample_cin_details,
            Header: sample_header,
        },
    )

    # Use .type2_issues to check for the result of .push_type2_issues() which you used above.
    issues_list = result.type2_issues
    assert len(issues_list) == 1
    # the function returns a list on NamedTuples where each NamedTuple contains (table, column_list, df_issues)
    # pick any table and check it's values. the tuple in location 1 will contain the Section47 columns because that's the second thing pushed above.
    issues = issues_list[0]

    # get table name and check it. Replace Section47 with the name of your table.
    issue_table = issues.table
    assert issue_table == CINdetails

    # check that the right columns were returned. Replace DateOfInitialCPC  with a list of your columns.
    issue_columns = issues.columns
    assert issue_columns == [CINreferralDate]

    # check that the location linking dataframe was formed properly.
    issue_rows = issues.row_df
    # replace 2 with the number of failing points you expect from the sample data.
    assert len(issue_rows) == 1
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
                    "child1",  # ChildID
                    "cinID1",  # CINdetailsID
                    # corresponding CPPstartDate
                    pd.to_datetime("26/10/1880", format="%d/%m/%Y", errors="coerce"),
                ),
                "ROW_ID": [0],
            },
        ]
    )
    assert issue_rows.equals(expected_df)

    # Check that the rule definition is what you wrote in the context above.

    # replace 8569 with the rule code and put the appropriate message in its place too.
    assert result.definition.code == 8569
    assert (
        result.definition.message
        == "A case with referral date before one working day prior to the collection start date must not be flagged as a no further action case"
    )
