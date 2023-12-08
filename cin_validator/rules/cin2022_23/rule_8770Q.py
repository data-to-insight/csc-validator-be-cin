from typing import Mapping

import pandas as pd

from cin_validator.rule_engine import CINTable, RuleContext, RuleType, rule_definition
from cin_validator.test_engine import run_rule

# Get tables and columns of interest from the CINTable object defined in rule_engine/__api.py

ChildIdentifiers = CINTable.ChildIdentifiers
CINdetails = CINTable.CINdetails

LAchildID = ChildIdentifiers.LAchildID
PersonBirthDate = ChildIdentifiers.PersonBirthDate
UPN = ChildIdentifiers.UPN
UPNunknown = ChildIdentifiers.UPNunknown
ReferralNFA = CINdetails.ReferralNFA

# Reference date in header is needed to define the period of census.
Header = CINTable.Header
ReferenceDate = Header.ReferenceDate


# define characteristics of rule
@rule_definition(
    # write the rule code here, in place of 8770Q
    code="8770Q",
    rule_type=RuleType.QUERY,
    # replace ChildIdentifiers with the value in the module column of the excel sheet corresponding to this rule .
    # Note that even if multiple tables are involved, one table will be named in the module column.
    module=CINTable.ChildIdentifiers,
    # replace the message with the corresponding value for this rule, gotten from the excel sheet.
    message="Please check and either amend data or provide a reason: UPN or reason UPN missing expected for a child who is more than 5 years old",
    # The column names tend to be the words within the < > signs in the github issue description.
    affected_fields=[
        UPNunknown,
        UPNunknown,
    ],
)
def validate(
    data_container: Mapping[CINTable, pd.DataFrame], rule_context: RuleContext
):
    # PREPARING DATA

    df_cid = data_container[ChildIdentifiers].copy()
    df_cin = data_container[CINdetails].copy()

    # Before you begin, rename the index so that the initial row positions can be kept intact.
    df_cid.index.name = "ROW_ID"
    df_cin.index.name = "ROW_ID"

    # Resetting the index causes the ROW_IDs to become columns of their respective DataFrames
    # so that they can come along when the merge is done.
    df_cid.reset_index(inplace=True)
    df_cin.reset_index(inplace=True)

    # get collection period
    header = data_container[Header]
    ref_date = header[ReferenceDate].iloc[0]
    ref_date_minus6 = ref_date - pd.DateOffset(years=6)

    # lOGIC
    # Implement rule logic as described by the Github issue.
    # Put the description as a comment above the implementation as shown.

    # If <ReferralNFA> (N00112) = 0 or false
    # and <PersonBirthDate> (N00066) is earlier than [<ReferenceDate> (N00603) minus 6 years]
    # then either <UPN> (N00001) or a valid <UPNunknown> (N00135) should be present

    # get only CINdetails where ReferralNFA is 0 or false
    df_cin = df_cin[
        (df_cin[ReferralNFA] == "0") | (df_cin[ReferralNFA].str.lower() == "false")
    ]

    # get only ChildIdentifiers where child is old enough to need a UPN
    df_cid = df_cid[(df_cid[PersonBirthDate] <= ref_date_minus6)]

    # merge ChildIdentifiers with filtered CINdetails and take only those that match
    df_merged = df_cid.merge(
        df_cin[[LAchildID, ReferralNFA]],
        on=[LAchildID],
        how="left",
        suffixes=["", "_cin"],
        indicator=True,
    )

    df_merged = df_merged[df_merged["_merge"] == "both"]

    # get only rows where UPN is not provided and UPNunknown is not a valid code
    no_upn = df_merged[UPN].isna()

    upnunknown_reasons = [
        # "UN1", - excluding UN1 as "Child is not of school age" is not applicable here
        "UN2",
        "UN3",
        "UN4",
        "UN5",
        "UN6",
        "UN7",
    ]

    valid_upnunknown = df_merged[UPNunknown].str.upper().isin(upnunknown_reasons)

    # get all the data that fits the failing condition.
    df_merged = df_merged[no_upn & ~valid_upnunknown].reset_index()

    # create an identifier for each error instance.
    df_merged["ERROR_ID"] = tuple(zip(df_merged[LAchildID]))

    # we can now map the suffixes columns to their corresponding source tables such that the failing ROW_IDs and ERROR_IDs exist per table.
    df_cid_issues = (
        df_cid.merge(df_merged, left_on="ROW_ID", right_on="ROW_ID")
        .groupby("ERROR_ID", group_keys=False)["ROW_ID"]
        .apply(list)
        .reset_index()
    )

    # Ensure that you maintain the ROW_ID, and ERROR_ID column names which are shown above. They are keywords in this project.
    rule_context.push_type_2(
        table=ChildIdentifiers, columns=[UPN, UPNunknown], row_df=df_cid_issues
    )


def test_validate():
    # Create some sample data such that some values pass the validation and some fail.
    sample_cid = pd.DataFrame(
        [
            {
                "LAchildID": "child1",  # 0 Ignore - under 5 years old
                "PersonBirthDate": "26/05/2000",
                "UPN": pd.NA,
                "UPNunknown": pd.NA,
            },
            {
                "LAchildID": "child2",  # 1 Pass - over 5 with UPN
                "PersonBirthDate": "26/05/1990",
                "UPN": "AAA",
                "UPNunknown": pd.NA,
            },
            {
                "LAchildID": "child3",  # 2 Pass - over 5 with valid UPNunknown
                "PersonBirthDate": "26/05/1990",
                "UPN": pd.NA,
                "UPNunknown": "UN2",
            },
            {
                "LAchildID": "child4",  # 3 Pass - over 5 with valid UPNunknown (lower case)
                "PersonBirthDate": "26/05/1990",
                "UPN": pd.NA,
                "UPNunknown": "un5",
            },
            {
                "LAchildID": "child5",  # 4 Fail - over 5 and no UPN or valid UPNunknown
                "PersonBirthDate": "26/05/1990",
                "UPN": pd.NA,
                "UPNunknown": pd.NA,
            },
            {
                "LAchildID": "child6",  # 5 Fail - over 5 and no UPN or valid UPNunknown (UN1 is not valid for >5yrs)
                "PersonBirthDate": "26/05/1990",
                "UPN": pd.NA,
                "UPNunknown": "UN1",
            },
            {
                "LAchildID": "child7",  # 6 Ignore - over 5 and no UPN or valid UPNunknown (UN8 is not valid) so should fail, BUT ReferralNFA doesn't meet requirement
                "PersonBirthDate": "26/05/1990",
                "UPN": pd.NA,
                "UPNunknown": "UN8",
            },
        ]
    )
    sample_cin_details = pd.DataFrame(
        [
            {"LAchildID": "child1", "ReferralNFA": "0"},
            {"LAchildID": "child2", "ReferralNFA": "false"},
            {"LAchildID": "child3", "ReferralNFA": "FALSE"},
            {"LAchildID": "child4", "ReferralNFA": "0"},
            {"LAchildID": "child5", "ReferralNFA": "0"},
            {"LAchildID": "child6", "ReferralNFA": "0"},
            {"LAchildID": "child6", "ReferralNFA": "1"},
            {"LAchildID": "child7", "ReferralNFA": "1"},
        ]
    )
    sample_header = pd.DataFrame(
        [{ReferenceDate: "31/03/2001"}]  # the census start date here will be 01/04/2000
    )

    # if rule requires columns containing date values, convert those columns to datetime objects first. Do it here in the test_validate function, not above.
    sample_cid[PersonBirthDate] = pd.to_datetime(
        sample_cid[PersonBirthDate], format="%d/%m/%Y", errors="coerce"
    )
    sample_header[ReferenceDate] = pd.to_datetime(
        sample_header[ReferenceDate], format="%d/%m/%Y", errors="coerce"
    )

    # Run the rule function, passing in our sample data.
    result = run_rule(
        validate,
        {
            ChildIdentifiers: sample_cid,
            CINdetails: sample_cin_details,
            Header: sample_header,
        },
    )

    # Use .type2_issues to check for the result of .push_type2_issues() which you used above.
    issues_list = result.type2_issues
    assert len(issues_list) == 1
    # the function returns a list on NamedTuples where each NamedTuple contains (table, column_list, df_issues)
    # pick any table and check it's values.
    issues = issues_list[0]

    issue_table = issues.table
    assert issue_table == ChildIdentifiers

    issue_columns = issues.columns
    assert issue_columns == [UPN, UPNunknown]

    # check that the location linking dataframe was formed properly.
    issue_rows = issues.row_df
    # replace 2 with the number of failing points you expect from the sample data.
    assert len(issue_rows) == 2
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
                "ERROR_ID": ("child5",),  # ChildID
                "ROW_ID": [4],
            },
            {
                "ERROR_ID": ("child6",),  # ChildID
                "ROW_ID": [5],
            },
        ]
    )
    assert issue_rows.equals(expected_df)

    # Check that the rule definition is what you wrote in the context above.

    # replace 8770Q with the rule code and put the appropriate message in its place too.
    assert result.definition.code == "8770Q"
    assert (
        result.definition.message
        == "Please check and either amend data or provide a reason: UPN or reason UPN missing expected for a child who is more than 5 years old"
    )
