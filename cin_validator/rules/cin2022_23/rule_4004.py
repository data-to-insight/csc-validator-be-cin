"""
Rule number: 4004
Module: CIN plan dates
Rule details: Within a <CINDetails> module, there must be only one <CINplanDates> group where the <CINPlanEnd Date> (N00690) is missing
Rule message: This child is showing more than one open CIN Plan, i.e. with no End Date

"""

from typing import Mapping

import pandas as pd

from cin_validator.rule_engine import CINTable, RuleContext, rule_definition
from cin_validator.rules.cin2022_23.rule_8925 import LAchildID
from cin_validator.test_engine import run_rule

# Get tables and columns of interest from the CINTable object defined in rule_engine/__api.py
CINplanDates = CINTable.CINplanDates
LAchildID = CINplanDates.LAchildID
CINPlanEndDate = CINplanDates.CINPlanEndDate
CINdetailsID = CINplanDates.CINdetailsID


# define characteristics of rule
@rule_definition(
    code=4004,
    module=CINTable.CINplanDates,
    message="This child is showing more than one open CIN Plan, i.e. with no End Date",
    affected_fields=[CINPlanEndDate],
)
def validate(
    data_container: Mapping[CINTable, pd.DataFrame], rule_context: RuleContext
):
    df = data_container[CINplanDates]
    # Rename and reset index
    df.index.name = "ROW_ID"
    df.reset_index(inplace=True)

    # There must be only one <CINplanDates> group where the <CINPlanEnd Date> (N00690) is missing

    df_check = df.copy()
    df_check = df_check[df_check[CINPlanEndDate].isna()]

    # Convert NAs to 1 and count by child
    df_check[CINPlanEndDate].fillna(1, inplace=True)
    df_check = (
        df_check.groupby([LAchildID, CINdetailsID])[CINPlanEndDate]
        .count()
        .reset_index()
    )

    # Find where there is more than 1 open end date
    df_check = df_check[df_check[CINPlanEndDate] > 1]
    issue_ids = tuple(zip(df_check[LAchildID], df_check[CINdetailsID]))

    df["ERROR_ID"] = tuple(zip(df[LAchildID], df[CINdetailsID]))
    df_issues = df[df.ERROR_ID.isin(issue_ids)]

    df_issues = df_issues.groupby("ERROR_ID")["ROW_ID"].apply(list).reset_index()
    rule_context.push_type_3(
        table=CINplanDates, columns=[CINPlanEndDate], row_df=df_issues
    )


def test_validate():
    # Create some sample data such that some values pass the validation and some fail.
    sample_CINplanDates = pd.DataFrame(
        [  # child1
            {
                LAchildID: "child1",
                CINdetailsID: "CINdetailsID1",
                CINPlanEndDate: pd.NA,  # 0 first nan date in group
            },
            {
                LAchildID: "child1",
                CINdetailsID: "CINdetailsID1",
                CINPlanEndDate: pd.NA,  # 1 second nan date in group
            },
            {  # won't be flagged because there is not more than one nan authorisation date in this group.
                LAchildID: "child1",
                CINdetailsID: "CINdetailsID2",
                CINPlanEndDate: pd.NA,  # 2
            },
            # child2
            {
                LAchildID: "child2",
                CINdetailsID: "CINdetailsID1",
                CINPlanEndDate: "26/05/2021",  # 3 ignored. not nan
            },
            {  # fail
                LAchildID: "child2",
                CINdetailsID: "CINdetailsID2",
                CINPlanEndDate: pd.NA,  # 4 first nan date in group
            },
            {  # fail
                LAchildID: "child2",
                CINdetailsID: "CINdetailsID2",
                CINPlanEndDate: pd.NA,  # 5 second nan date in group
            },
        ]
    )
    # if rule requires columns containing date values, convert those columns to
    # datetime objects first. Do it here in the test_validate function, not above.
    sample_CINplanDates[CINPlanEndDate] = pd.to_datetime(
        sample_CINplanDates[CINPlanEndDate],
        format="%d/%m/%Y",
        errors="coerce",
    )

    # Run rule function passing in our sample data
    result = run_rule(validate, {CINplanDates: sample_CINplanDates})

    # Use .type3_issues to check for the result of .push_type3_issues() which you used above.
    issues = result.type3_issues

    # get table name and check it. Replace CINplanDates with the name of your table.
    issue_table = issues.table
    assert issue_table == CINplanDates

    # check that the right columns were returned. Replace CINPlanEndDate with a list of your columns.
    issue_columns = issues.columns
    assert issue_columns == [CINPlanEndDate]

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
                    "child1",
                    "CINdetailsID1",
                ),
                "ROW_ID": [0, 1],
            },
            {
                "ERROR_ID": (
                    "child2",
                    "CINdetailsID2",
                ),
                "ROW_ID": [4, 5],
            },
        ]
    )
    assert issue_rows.equals(expected_df)

    # Check that the rule definition is what you wrote in the context above.

    # replace 8925 with the rule code and put the appropriate message in its place too.
    assert result.definition.code == 4004
    assert (
        result.definition.message
        == "This child is showing more than one open CIN Plan, i.e. with no End Date"
    )
