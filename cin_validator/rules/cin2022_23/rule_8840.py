from typing import Mapping

import pandas as pd

from cin_validator.rule_engine import CINTable, RuleContext, rule_definition
from cin_validator.test_engine import run_rule

# Get tables and columns of interest from the CINTable object defined in rule_engine/__api.py
# Replace ChildProtectionPlans with the table name, and LAChildID with the column name you want.

ChildProtectionPlans = CINTable.ChildProtectionPlans
LAchildID = ChildProtectionPlans.LAchildID
CPPstartDate = ChildProtectionPlans.CPPstartDate
CPPendDate = ChildProtectionPlans.CPPendDate


# define characteristics of rule
@rule_definition(
    # write the rule code here, in place of '8840'
    code="8840",
    # replace ChildProtectionPlans with the value in the module column of the excel sheet corresponding to this rule .
    module=CINTable.ChildProtectionPlans,
    # replace the message with the corresponding value for this rule, gotten from the excel sheet.
    message="Child Protection Plan cannot start and end on the same day",
    # The column names tend to be the words within the < > signs in the github issue description.
    affected_fields=[ChildProtectionPlans, CPPstartDate, CPPendDate],
)
def validate(
    data_container: Mapping[CINTable, pd.DataFrame], rule_context: RuleContext
):
    # Replace ChildProtectionPlans with the name of the table you need.
    df = data_container[ChildProtectionPlans]
    # Before you begin, rename the index so that the initial row positions can be kept intact.
    df.index.name = "ROW_ID"

    # LOGIC
    # Within a <ChildProtectionPlans> group, <CPPstartDate> (N00105) must not be the same as the <CPPendDate> (N00115)

    #  Determine if the dates are the same by finding if the difference between dates is 0
    condition = df["CPPstartDate"] == df["CPPendDate"]
    # get all the data that fits the failing condition. Reset the index so that ROW_ID now becomes a column of df
    df_issues = df[condition].reset_index()

    # SUBMIT ERRORS
    # Generate a unique ID for each instance of an error. In this case,
    # - If only LAchildID is used as an identifier, multiple instances of the error on a child will be understood as 1 instance.
    # We don't want that because in reality, a child can have multiple instances of an error.
    # - If we use the LAchildID-CPPstartDate combination, that artificially cancels out the instances where a start date repeats for the same child.
    # Another rule checks for that condition. Not this one.
    # - It is very unlikely that a combination of LAchildID-CPPstartDate-CPPendDate will repeat in the DataFrame.
    # Hence, it can be used as a unique identifier of the row.

    # Replace CPPstartDate and CPPendDate below with the columns concerned in your rule.
    link_id = tuple(
        zip(df_issues[LAchildID], df_issues[CPPstartDate], df_issues[CPPendDate])
    )
    df_issues["ERROR_ID"] = link_id
    df_issues = (
        df_issues.groupby("ERROR_ID", group_keys=False)["ROW_ID"]
        .apply(list)
        .reset_index()
    )
    # Ensure that you do not change the ROW_ID, and ERROR_ID column names which are shown above. They are keywords in this project.
    rule_context.push_type_1(
        table=ChildProtectionPlans, columns=[CPPstartDate, CPPendDate], row_df=df_issues
    )


def test_validate():
    # Create some sample data such that some values pass the validation and some fail.

    #  Fails rows 0, 1, and 3
    child_protection_plans = pd.DataFrame(
        {
            "LAchildID": ["child1", "child2", "child3", "child4", "child5"],
            "CPPstartDate": [
                "08/10/1989",
                "05/12/1993",
                "05/12/1993",
                "05/12/1997",
                pd.NA,
            ],
            "CPPendDate": [
                "08/10/1989",
                "05/12/1993",
                "12/09/2022",
                "05/12/1997",
                pd.NA,
            ],
        }
    )
    child_protection_plans["CPPstartDate"] = pd.to_datetime(
        child_protection_plans["CPPstartDate"], format="%d/%m/%Y", errors="coerce"
    )
    child_protection_plans["CPPendDate"] = pd.to_datetime(
        child_protection_plans["CPPendDate"], format="%d/%m/%Y", errors="coerce"
    )

    # Run rule function passing in our sample data
    result = run_rule(validate, {ChildProtectionPlans: child_protection_plans})

    # The result contains a NamedTuple of issues encountered
    issues = result.type1_issues

    # get table name and check it. Replace ChildProtectionPlans with the name of your table.
    issue_table = issues.table
    assert issue_table == ChildProtectionPlans

    # check that the right columns were returned. Replace CPPstartDate and CPPendDate with a list of your columns.
    issue_columns = issues.columns
    assert issue_columns == [CPPstartDate, CPPendDate]

    # check that the location linking dataframe was formed properly.
    issue_rows = issues.row_df

    # replace 2 with the number of failing points you expect from the sample data.
    assert len(issue_rows) == 3
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
                    pd.to_datetime("08/10/1989", format="%d/%m/%Y", errors="coerce"),
                    pd.to_datetime("08/10/1989", format="%d/%m/%Y", errors="coerce"),
                ),
                "ROW_ID": [0],
            },
            {
                "ERROR_ID": (
                    "child2",
                    pd.to_datetime("05/12/1993", format="%d/%m/%Y", errors="coerce"),
                    pd.to_datetime("05/12/1993", format="%d/%m/%Y", errors="coerce"),
                ),
                "ROW_ID": [1],
            },
            {
                "ERROR_ID": (
                    "child4",
                    pd.to_datetime("05/12/1997", format="%d/%m/%Y", errors="coerce"),
                    pd.to_datetime("05/12/1997", format="%d/%m/%Y", errors="coerce"),
                ),
                "ROW_ID": [3],
            },
        ]
    )
    assert issue_rows.equals(expected_df)

    # Check that the rule definition is what you wrote in the context above.

    # replace '8840' with the rule code and put the appropriate message in its place too.
    assert result.definition.code == "8840"
    assert (
        result.definition.message
        == "Child Protection Plan cannot start and end on the same day"
    )
