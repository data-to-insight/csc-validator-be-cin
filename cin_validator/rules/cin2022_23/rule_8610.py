from typing import Mapping

import pandas as pd

from cin_validator.rule_engine import (
    CINTable,
    IssueLocator,
    RuleContext,
    rule_definition,
)
from cin_validator.test_engine import run_rule

# Get tables and columns of interest from the CINTable object defined in rule_engine/__api.py
# Replace ChildProtectionPlans with the table name, and LAChildID with the column name you want.

CINdetails = CINTable.CINdetails
LAchildID = CINdetails.LAchildID
ReferralNFA = CINdetails.ReferralNFA
PrimaryNeedCode = CINdetails.PrimaryNeedCode
CINdetailsID = CINdetails.CINdetailsID

# define characteristics of rule
@rule_definition(
    # write the rule code here, in place of 8840
    code=8610,
    # replace ChildProtectionPlans with the value in the module column of the excel sheet corresponding to this rule .
    module=CINTable.CINdetails,
    # replace the message with the corresponding value for this rule, gotten from the excel sheet.
    message="Primary Need code is missing for a referral which led to further",
    # The column names tend to be the words within the < > signs in the github issue description.
    affected_fields=[ReferralNFA, PrimaryNeedCode],
)
def validate(
    data_container: Mapping[CINTable, pd.DataFrame], rule_context: RuleContext
):
    # Replace ChildProtectionPlans with the name of the table you need.
    df = data_container[CINdetails]
    # Before you begin, rename the index so that the initial row positions can be kept intact.
    df.index.name = "ROW_ID"

    #  Determine if the dates are the same by finding if the difference between dates is 0
    falseorzero = ["false", "0"]
    condition = (df[ReferralNFA].isin(falseorzero)) & (df[PrimaryNeedCode].isna())
    # get all the data that fits the failing condition. Reset the index so that ROW_ID now becomes a column of df
    df_issues = df[condition].reset_index()

    link_id = tuple(zip(df_issues[LAchildID], df_issues[CINdetailsID]))
    df_issues["ERROR_ID"] = link_id
    df_issues = (
        df_issues.groupby("ERROR_ID", group_keys=False)["ROW_ID"]
        .apply(list)
        .reset_index()
    )
    # Ensure that you do not change the ROW_ID, and ERROR_ID column names which are shown above. They are keywords in this project.
    rule_context.push_type_1(
        table=CINdetails, columns=[ReferralNFA, PrimaryNeedCode], row_df=df_issues
    )


def test_validate():
    # Create some sample data such that some values pass the validation and some fail.

    #  Fails rows 0, 1, and 3
    sample_cin = pd.DataFrame(
        {
            "LAchildID": ["child1", "child2", "child3", "child4", "child5"],
            "ReferralNFA": [
                "false",
                "0",
                "1",
                "true",
                pd.NA,
            ],
            "PrimaryNeedCode": [
                pd.NA,
                pd.NA,
                "12/09/2022",
                "05/12/1997",
                pd.NA,
            ],
            "CINdetailsID": [
                "ID1",
                "ID2",
                "ID3",
                "ID4",
                "ID5",
            ],
        }
    )

    # Run rule function passing in our sample data
    result = run_rule(validate, {CINdetails: sample_cin})

    # The result contains a NamedTuple of issues encountered
    issues = result.type1_issues

    # get table name and check it. Replace ChildProtectionPlans with the name of your table.
    issue_table = issues.table
    assert issue_table == CINdetails
    # check that the right columns were returned. Replace CPPstartDate and CPPendDate with a list of your columns.
    issue_columns = issues.columns
    assert issue_columns == [ReferralNFA, PrimaryNeedCode]

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
                    "child1",
                    "ID1",
                ),
                "ROW_ID": [0],
            },
            {
                "ERROR_ID": ("child2", "ID2"),
                "ROW_ID": [1],
            },
        ]
    )
    assert issue_rows.equals(expected_df)

    # Check that the rule definition is what you wrote in the context above.

    # replace 8840 with the rule code and put the appropriate message in its place too.
    assert result.definition.code == 8610
    assert (
        result.definition.message
        == "Primary Need code is missing for a referral which led to further"
    )
