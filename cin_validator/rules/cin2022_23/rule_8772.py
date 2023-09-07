from typing import Mapping

import pandas as pd

from cin_validator.rule_engine import CINTable, RuleContext, rule_definition
from cin_validator.test_engine import run_rule

# Get tables and columns of interest from the CINTable object defined in rule_engine/__api.py

ChildIdentifiers = CINTable.ChildIdentifiers
LAchildID = ChildIdentifiers.LAchildID
UPNunknown = ChildIdentifiers.UPNunknown

CINdetails = CINTable.CINdetails
LAchildID = CINdetails.LAchildID
ReferralNFA = CINdetails.ReferralNFA


@rule_definition(
    code="8772",
    module=CINTable.ChildIdentifiers,
    message="UPN unknown reason is UN7 (Referral with no further action) but at least one CIN details is a referral going on to further action",
    affected_fields=[
        UPNunknown,
        ReferralNFA,
    ],
)
def validate(
    data_container: Mapping[CINTable, pd.DataFrame], rule_context: RuleContext
):
    df_upn = data_container[ChildIdentifiers].copy()
    df_refs = data_container[CINdetails].copy()

    # Before you begin, rename the index so that the initial row positions can be kept intact.
    df_upn.index.name = "ROW_ID"
    df_refs.index.name = "ROW_ID"

    # Resetting the index causes the ROW_IDs to become columns of their respective DataFrames
    # so that they can come along when the merge is done.
    df_upn.reset_index(inplace=True)
    df_refs.reset_index(inplace=True)

    # lOGIC
    # Implement rule logic as described by the Github issue.
    # Put the description as a comment above the implementation as shown.

    # If <UPNunknown> (N00135) is UN7 then all of the CIN details must have <ReferralNFA> (N00112) = 1 or true

    df_merged = df_upn.merge(
        df_refs,
        on=["LAchildID"],
        how="left",
        suffixes=("_upn", "_refs"),
    )

    #  Get rows where UPNunknown is equal to 'UN7'
    condition_1 = df_merged[UPNunknown] == "UN7"

    #  Get rows where ReferralNFA is equal to either 'True' or '1'
    trueor1 = ["true", "1"]
    condition_2 = df_merged[ReferralNFA].str.lower().isin(trueor1)

    # Combine the results of the two rows
    df_merged = df_merged[condition_1 & ~condition_2].reset_index()

    # create an identifier for each error instance.
    df_merged["ERROR_ID"] = tuple(zip(df_merged[LAchildID], df_merged[ReferralNFA]))

    # we can now map the suffixes columns to their corresponding source tables such that the failing ROW_IDs and ERROR_IDs exist per table.
    df_upn_issues = (
        df_upn.merge(df_merged, left_on="ROW_ID", right_on="ROW_ID_upn")
        .groupby("ERROR_ID", group_keys=False)["ROW_ID"]
        .apply(list)
        .reset_index()
    )
    df_refs_issues = (
        df_refs.merge(df_merged, left_on="ROW_ID", right_on="ROW_ID_refs")
        .groupby("ERROR_ID", group_keys=False)["ROW_ID"]
        .apply(list)
        .reset_index()
    )

    # Ensure that you maintain the ROW_ID, and ERROR_ID column names which are shown above. They are keywords in this project.
    rule_context.push_type_2(
        table=ChildIdentifiers, columns=[UPNunknown], row_df=df_upn_issues
    )
    rule_context.push_type_2(
        table=CINdetails, columns=[ReferralNFA], row_df=df_refs_issues
    )


def test_validate():
    # Create some sample data such that some values pass the validation and some fail.
    sample_upn = pd.DataFrame(
        [
            {
                "LAchildID": "child1",
                "UPNunknown": "UN6",  # Ignore: code is not UN7
            },
            {
                "LAchildID": "child2",
                "UPNunknown": "UN7",
            },
            {
                "LAchildID": "child3",
                "UPNunknown": "UN7",
            },
            {
                "LAchildID": "child4",
                "UPNunknown": "UN7",
            },
            {
                "LAchildID": "child5",
                "UPNunknown": "UN4",  # Ignore: code is not UN7
            },
        ]
    )

    sample_refs = pd.DataFrame(
        [
            {
                "LAchildID": "child1",
                "ReferralNFA": "True",  # 0 Ignore: upn is not UN7
            },
            {
                "LAchildID": "child2",
                "ReferralNFA": "1",  # 1 Pass as Referral Code is True
            },
            {
                "LAchildID": "child3",
                "ReferralNFA": pd.NA,  # 2 Fail as Referral Code is NULL
            },
            {
                "LAchildID": "child4",
                "ReferralNFA": "nottrue",  # 3 Fail: Referral Code is neither "1" not "true"
            },
            {
                "LAchildID": "child5",
                "ReferralNFA": "True",  # 4 Ignore: upn is not UN7
            },
        ]
    )

    # Run the rule function, passing in our sample data.
    result = run_rule(
        validate,
        {
            ChildIdentifiers: sample_upn,
            CINdetails: sample_refs,
        },
    )

    # Use .type2_issues to check for the result of .push_type2_issues() which you used above.
    issues_list = result.type2_issues
    assert len(issues_list) == 2

    # the function returns a list on NamedTuples where each NamedTuple contains (table, column_list, df_issues)
    # pick any table and check it's values.
    issues = issues_list[1]

    # get table name and check it. Replace CINdetails with the name of your table.
    issue_table = issues.table
    assert issue_table == CINdetails

    # check that the right columns were returned. Replace ReferralNFA with a list of your columns.
    issue_columns = issues.columns
    assert issue_columns == [ReferralNFA]

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
                "ERROR_ID": (
                    "child3",  # ChildID
                    pd.NA,  # ReferralNFA
                ),
                "ROW_ID": [2],
            },
            {
                "ERROR_ID": (
                    "child4",  # ChildID
                    "nottrue",  # ReferralNFA
                ),
                "ROW_ID": [3],
            },
        ]
    )
    assert issue_rows.equals(expected_df)

    # Check that the rule definition is what you wrote in the context above.

    # replace '8772' with the rule code and put the appropriate message in its place too.
    assert result.definition.code == "8772"
    assert (
        result.definition.message
        == "UPN unknown reason is UN7 (Referral with no further action) but at least one CIN details is a referral going on to further action"
    )
