from typing import Mapping

import pandas as pd

from cin_validator.rule_engine import CINTable, RuleContext, rule_definition
from cin_validator.test_engine import run_rule
from cin_validator.utils import make_census_period

# Get tables and columns of interest from the CINTable object defined in rule_engine/__api.py

CINdetails = CINTable.CINdetails
LAchildID = CINdetails.LAchildID
CINdetailsID = CINdetails.CINdetailsID
CINreferralDate = CINdetails.CINreferralDate
CINclosureDate = CINdetails.CINclosureDate
ReferralNFA = CINdetails.ReferralNFA

Header = CINTable.Header
ReferenceDate = Header.ReferenceDate


# define characteristics of rule
@rule_definition(
    # write the rule code here
    code="8820",
    # replace CINdetails with the value in the module column of the excel sheet corresponding to this rule .
    # Note that even if multiple tables are involved, one table will be named in the module column.
    module=CINTable.CINdetails,
    # replace the message with the corresponding value for this rule, gotten from the excel sheet.
    message="The dates on the CIN episodes for this child overlap",
    # The column names tend to be the words within the < > signs in the github issue description.
    affected_fields=[
        CINreferralDate,
        CINclosureDate,
    ],
)
def validate(
    data_container: Mapping[CINTable, pd.DataFrame], rule_context: RuleContext
):
    # PREPARING DATA

    # Replace CINdetails with the name of the table you need.
    df_cin = data_container[CINdetails]
    df_cin2 = data_container[CINdetails]

    # Before you begin, rename the index so that the initial row positions can be kept intact.
    df_cin.index.name = "ROW_ID"
    df_cin2.index.name = "ROW_ID"

    # Resetting the index causes the ROW_IDs to become columns of their respective DataFrames
    # so that they can come along when the merge is done.
    df_cin.reset_index(inplace=True)
    df_cin2.reset_index(inplace=True)

    # ReferenceDate exists in the header table so we get header table too.
    df_ref = data_container[Header]
    ref_date_series = df_ref[ReferenceDate]

    # the make_census_period function generates the start and end date so that you don't have to do it each time.
    collection_start, reference_date = make_census_period(ref_date_series)

    # lOGIC
    # Implement rule logic as described by the Github issue.
    # Put the description as a comment above the implementation as shown.

    # If more than one <CINdetails> module is present for the same child, then for each module the <CINreferralDate> (N00100) must not fall;
    # a) between the <CINreferralDate> (N00100) and the <CINclosureDate> (N00102) if present; and
    # b) between the <CINreferralDate> (N00100) and the <ReferenceDate> (N00603) if the <CINclosureDate> is not present and <ReferralNFA> (N00112) = false or 0
    #
    # Note: the effect of this rule is that there cannot be overlapping referrals, although the end date of one referral may be the same as the start date of the following referral.

    #  Create dataframes which only have rows with CINreferralDate, and which should have one plan per row.
    df_cin = df_cin[df_cin[CINreferralDate].notna()]
    df_cin2 = df_cin2[df_cin2[CINreferralDate].notna()]

    #  Merge tables to test for overlaps
    df_merged = df_cin.merge(
        df_cin2,
        on=["LAchildID"],
        how="left",
        suffixes=("_cin", "_cin2"),
    )

    # Exclude rows where the ROW_ID is the same on both sides
    df_merged = df_merged[(df_merged["ROW_ID_cin"] != df_merged["ROW_ID_cin2"])]

    # Determine overlaps
    cin_started_after_start = (
        df_merged["CINreferralDate_cin"] >= df_merged["CINreferralDate_cin2"]
    )
    cin_started_before_end = (
        df_merged["CINreferralDate_cin"] < df_merged["CINclosureDate_cin2"]
    ) & df_merged["CINclosureDate_cin2"].notna()

    cin_started_before_refdate = (
        (df_merged["CINreferralDate_cin"] < reference_date)
        & df_merged["CINclosureDate_cin2"].isna()
        & df_merged["ReferralNFA_cin2"].str.lower().isin(["false", "0"])
    )

    df_merged = df_merged[
        cin_started_after_start & (cin_started_before_end | cin_started_before_refdate)
    ].reset_index()

    # create an identifier for each error instance.
    # In this case, the rule is checked for each CPPstartDate, in each CPplanDates group (differentiated by CP dates), in each child (differentiated by LAchildID)
    df_merged["ERROR_ID"] = tuple(
        zip(
            df_merged[LAchildID],
            df_merged["CINdetailsID_cin"],
            df_merged["CINdetailsID_cin2"],
        )
    )

    # The merges were done on copies of cpp_df so that the column names in dataframes themselves aren't affected by the suffixes.
    # we can now map the suffixes columns to their corresponding source tables such that the failing ROW_IDs and ERROR_IDs exist per table.
    df_cin_issues = (
        df_cin.merge(df_merged, left_on="ROW_ID", right_on="ROW_ID_cin")
        .groupby("ERROR_ID", group_keys=False)["ROW_ID"]
        .apply(list)
        .reset_index()
    )
    df_cin2_issues = (
        df_cin2.merge(df_merged, left_on="ROW_ID", right_on="ROW_ID_cin2")
        .groupby("ERROR_ID", group_keys=False)["ROW_ID"]
        .apply(list)
        .reset_index()
    )

    # Ensure that you maintain the ROW_ID, and ERROR_ID column names which are shown above. They are keywords in this project.
    rule_context.push_type_3(
        table=CINdetails, columns=[CINreferralDate], row_df=df_cin_issues
    )
    rule_context.push_type_3(
        table=CINdetails,
        columns=[CINreferralDate, CINclosureDate],
        row_df=df_cin2_issues,
    )


def test_validate():
    # Create some sample data such that some values pass the validation and some fail.
    sample_header = pd.DataFrame(
        [{ReferenceDate: "31/03/2001"}]  # the census start date here will be 01/04/2000
    )

    sample_cin = pd.DataFrame(
        [  # child1
            {
                "LAchildID": "child1",
                "CINreferralDate": "26/05/2000",  # 0 Pass: not between "26/08/2000" and "31/03/2001"
                "CINclosureDate": "26/10/2000",
                "CINdetailsID": "cinID1",
                "ReferralNFA": "true",
            },
            {
                "LAchildID": "child1",
                "CINreferralDate": "26/08/2000",  # 1 Fail: between "26/05/2000" and "26/10/2000"
                "CINclosureDate": pd.NA,
                "CINdetailsID": "cinID12",
                "ReferralNFA": "0",
            },
            # child2
            {
                "LAchildID": "child2",
                "CINreferralDate": "26/05/2000",  # 2 pass, not between
                "CINclosureDate": "25/10/2000",
                "CINdetailsID": "cinID2",
                "ReferralNFA": "true",
            },
            {
                "LAchildID": "child2",
                "CINreferralDate": "26/10/2000",  # 3 pass, not between
                "CINclosureDate": "26/12/2000",
                "CINdetailsID": "cinID22",
                "ReferralNFA": "true",
            },
            # child3
            {
                "LAchildID": "child3",
                "CINreferralDate": "26/05/2000",  # 4 Pass: not between "26/08/2000" and "26/10/2000"
                "CINclosureDate": "26/10/2001",
                "CINdetailsID": "cinID3",
                "ReferralNFA": "true",
            },
            {
                "LAchildID": "child3",  # 5 Fail: between "26/05/2000" and "26/10/2001"
                "CINreferralDate": "26/08/2000",
                "CINclosureDate": "26/10/2000",
                "CINdetailsID": "cinID32",
                "ReferralNFA": "true",
            },
            # child4
            {
                "LAchildID": "child4",
                "CINreferralDate": "26/10/2000",  # 6 Ignore: between "26/09/2000" and ReferenceDate but ReferralNFA is true
                "CINclosureDate": "31/03/2001",
                "CINdetailsID": "cinID4",
                "ReferralNFA": "true",
            },
            {
                "LAchildID": "child4",
                "CINreferralDate": "26/09/2000",  # 7 Pass: not between "26/10/2000" and "31/03/2001"
                "CINclosureDate": pd.NA,
                "CINdetailsID": "cinID42",
                "ReferralNFA": "true",
            },
            # child 5
            {
                "LAchildID": "child5",
                "CINreferralDate": "08/07/2000",
                "CINclosureDate": "23/08/2000",
                "CINdetailsID": "cinID4",
                "ReferralNFA": "false",
            },
            {
                "LAchildID": "child5",
                "CINreferralDate": "05/05/2000",
                "CINclosureDate": "08/07/2000",
                "CINdetailsID": "cinID5",
                "ReferralNFA": "false",
            },
            # child 6 - to account for duplicated entries as per issue 372
            {
                "LAchildID": "child6",  # 10, fail, duplicated
                "CINreferralDate": "05/05/2000",
                "CINclosureDate": "08/07/2000",
                "CINdetailsID": "cinID5",
                "ReferralNFA": "0",
            },
            {
                "LAchildID": "child6",  # 11, fail, duplicated
                "CINreferralDate": "05/05/2000",
                "CINclosureDate": "08/07/2000",
                "CINdetailsID": "cinID5",
                "ReferralNFA": "0",
            },
        ]
    )

    # If rule requires columns containing date values, convert those columns to datetime objects first. Do it here in the test_validate function, not above.
    sample_cin[CINreferralDate] = pd.to_datetime(
        sample_cin[CINreferralDate], format="%d/%m/%Y", errors="coerce"
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

    issues_list = result.type3_issues
    assert len(issues_list) == 2
    # the function returns a list on NamedTuples where each NamedTuple contains (table, column_list, df_issues)
    issues = issues_list[0]

    # get table name and check it. Replace CINdetails with the name of your table.
    issue_table = issues.table
    assert issue_table == CINdetails

    # check that the right columns were returned. Replace CINreferralDate  with a list of your columns.
    issue_columns = issues.columns
    assert issue_columns == [CINreferralDate]

    # check that the location linking dataframe was formed properly.
    issue_rows = issues.row_df
    # replace 2 with the number of failing points you expect from the sample data.
    assert len(issue_rows) == 3

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
                    "cinID12",
                    "cinID1",
                ),
                "ROW_ID": [1],
            },
            {
                "ERROR_ID": (
                    "child3",
                    "cinID32",
                    "cinID3",
                ),
                "ROW_ID": [5],
            },
            {
                "ERROR_ID": (
                    "child6",
                    "cinID5",
                    "cinID5",
                ),
                "ROW_ID": [10, 11],
            },
        ]
    )
    assert issue_rows.equals(expected_df)

    # Check that the rule definition is what you wrote in the context above.

    # replace '8820' with the rule code and put the appropriate message in its place too.
    assert result.definition.code == "8820"
    assert (
        result.definition.message
        == "The dates on the CIN episodes for this child overlap"
    )
