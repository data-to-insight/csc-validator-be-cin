from typing import Mapping

import pandas as pd

from cin_validator.rule_engine import CINTable, RuleContext, rule_definition
from cin_validator.test_engine import run_rule

# Get tables and columns of interest from the CINTable object defined in rule_engine/__api.py

ChildProtectionPlans = CINTable.ChildProtectionPlans
LAchildID_cpp = ChildProtectionPlans.LAchildID
CINdetailsID_cpp = ChildProtectionPlans.CINdetailsID

CINdetails = CINTable.CINdetails
ReferralNFA = CINdetails.ReferralNFA
LAchildID_details = CINdetails.LAchildID
CINdetailsID_details = CINdetails.CINdetailsID


# define characteristics of rule
@rule_definition(
    # write the rule code here
    code="8832",
    # replace ChildProtectionPlans with the value in the module column of the excel sheet corresponding to this rule .
    # Note that even if multiple tables are involved, one table will be named in the module column.
    module=CINTable.ChildProtectionPlans,
    # replace the message with the corresponding value for this rule, gotten from the excel sheet.
    message="Child Protection details provided for a referral with no further action.",
    # The column names tend to be the words within the < > signs in the github issue description.
    affected_fields=[
        CINdetailsID_cpp,
        ReferralNFA,
    ],
)
def validate(
    data_container: Mapping[CINTable, pd.DataFrame], rule_context: RuleContext
):
    # PREPARING DATA

    # Replace ChildProtectionPlans with the name of the table you need.
    df_cpp = data_container[ChildProtectionPlans]
    df_CINdetails = data_container[CINdetails]

    # Before you begin, rename the index so that the initial row positions can be kept intact.
    df_cpp.index.name = "ROW_ID"
    df_CINdetails.index.name = "ROW_ID"

    # Resetting the index causes the ROW_IDs to become columns of their respective DataFrames
    # so that they can come along when the merge is done.
    df_cpp.reset_index(inplace=True)
    df_CINdetails.reset_index(inplace=True)

    # If a <CINdetails> module has <ReferralNFA> (N00112) = true or 1, then there should be no Child Protection module present

    # Excluding rows with false or 0 <ReferralNFA> to fix bug where they were flagged as failing
    # TODO remove this step when proven to be redundant.
    df_CINdetails = df_CINdetails[
        ~(df_CINdetails[ReferralNFA].str.lower() == "false")
        | ~(df_CINdetails[ReferralNFA].astype(str) == "0")
    ]

    df_CINdetails = df_CINdetails[
        (df_CINdetails[ReferralNFA].str.lower() == "true")
        | (df_CINdetails[ReferralNFA].astype(str) == "1")
    ]

    #  Merge tables to get corresponding CP plan group and reviews
    df_merged = df_CINdetails.merge(
        df_cpp,
        on=["LAchildID", "CINdetailsID"],
        how="inner",
        suffixes=("_cpp", "_cin"),
    )
    # any rows found in df_merged are ones where <ReferralNFA> (N00112) = true or 1 and yet the child existed in the CINdetails table.
    df_merged = df_merged.reset_index()

    # create an identifier for each error instance.
    # In this case, the rule is checked for each CPPstartDate, in each CPplanDates group (differentiated by CP dates), in each child (differentiated by LAchildID)
    # So, a combination of LAchildID, CPPstartDate and CPPreviewDate identifies and error instance.
    df_merged["ERROR_ID"] = tuple(
        zip(
            df_merged["LAchildID"],
            df_merged["CINdetailsID"],
        )
    )

    # The merges were done on copies of cpp_df and reviews_df so that the column names in dataframes themselves aren't affected by the suffixes.
    # we can now map the suffixes columns to their corresponding source tables such that the failing ROW_IDs and ERROR_IDs exist per table.
    df_cpp_issues = (
        df_cpp.merge(df_merged, left_on="ROW_ID", right_on="ROW_ID_cpp")
        .groupby("ERROR_ID", group_keys=False)["ROW_ID"]
        .apply(list)
        .reset_index()
    )
    df_cin_issues = (
        df_CINdetails.merge(df_merged, left_on="ROW_ID", right_on="ROW_ID_cin")
        .groupby("ERROR_ID", group_keys=False)["ROW_ID"]
        .apply(list)
        .reset_index()
    )

    # Ensure that you maintain the ROW_ID, and ERROR_ID column names which are shown above. They are keywords in this project.
    rule_context.push_type_2(
        table=ChildProtectionPlans, columns=[LAchildID_cpp], row_df=df_cpp_issues
    )
    rule_context.push_type_2(
        table=CINdetails, columns=[ReferralNFA], row_df=df_cin_issues
    )


def test_validate():
    # Create some sample data such that some values pass the validation and some fail.
    sample_cpp = pd.DataFrame(
        [
            {
                "LAchildID": "child1",
                "CINdetailsID": "CDID1",
            },
            {
                "LAchildID": "child1",
                "CINdetailsID": "CDID2",
            },
            # {
            #     "LAchildID": "child3",
            #     "CINdetailsID": "CDID6",
            # },
            {
                "LAchildID": "child4",  # ignored
                "CINdetailsID": "CDID0",
            },
            {
                "LAchildID": "child5",  # ignored, ReferralNFA is neither "1" nor "true"
                "CINdetailsID": "CDID0",
            },
        ]
    )
    sample_cin = pd.DataFrame(
        [
            {
                "LAchildID": "child1",  # Fail, module present
                "CINdetailsID": "CDID1",
                "ReferralNFA": "1",
            },
            {
                "LAchildID": "child1",  # Pass, no referralNFA
                "CINdetailsID": "CDID2",
                "ReferralNFA": pd.NA,
            },
            {
                "LAchildID": "child3",  # Pass, no module
                "CINdetailsID": "CDID6",
                "ReferralNFA": "1",
            },
            {
                "LAchildID": "child4",  # ignored, ReferralNFA is neither "1" nor "true"
                "CINdetailsID": "CDID0",
                "ReferralNFA": "false",
            },
            {
                "LAchildID": "child5",  # ignored, ReferralNFA is neither "1" nor "true"
                "CINdetailsID": "CDID0",
                "ReferralNFA": 0,
            },
        ]
    )

    # Run the rule function, passing in our sample data.
    result = run_rule(
        validate,
        {
            ChildProtectionPlans: sample_cpp,
            CINdetails: sample_cin,
        },
    )

    # Use .type2_issues to check for the result of .push_type2_issues() which you used above.
    issues_list = result.type2_issues
    assert len(issues_list) == 2
    # the function returns a list on NamedTuples where each NamedTuple contains (table, column_list, df_issues)
    # pick any table and check it's values. the tuple in location 1 will contain the CINdetails columns because that's the second thing pushed above.
    issues = issues_list[1]

    # get table name and check it. Replace Reviews with the name of your table.
    issue_table = issues.table
    assert issue_table == CINdetails

    issue_columns = issues.columns
    assert issue_columns == [ReferralNFA]

    # check that the location linking dataframe was formed properly.
    issue_rows = issues.row_df
    # replace 1 with the number of failing points you expect from the sample data.
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
                    "CDID1",
                ),
                "ROW_ID": [0],
            },
        ]
    )
    assert issue_rows.equals(expected_df)

    # Check that the rule definition is what you wrote in the context above.

    # replace '8832' with the rule code and put the appropriate message in its place too.
    assert result.definition.code == "8832"
    assert (
        result.definition.message
        == "Child Protection details provided for a referral with no further action."
    )
