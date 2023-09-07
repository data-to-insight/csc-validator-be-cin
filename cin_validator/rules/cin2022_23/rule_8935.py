from typing import Mapping

import pandas as pd

from cin_validator.rule_engine import CINTable, RuleContext, rule_definition
from cin_validator.test_engine import run_rule

# Get tables and columns of interest from the CINTable object defined in rule_engine/__api.py
ChildProtectionPlans = CINTable.ChildProtectionPlans
LAchildID = ChildProtectionPlans.LAchildID
CPPendDate = ChildProtectionPlans.CPPendDate
CPPID = ChildProtectionPlans.CPPID


# define characteristics of rule
@rule_definition(
    # write the rule code here, in place of '8935'
    code="8935",
    # replace ChildProtectionPlans with the value in the module column of the excel sheet corresponding to this rule .
    module=CINTable.ChildProtectionPlans,
    # replace the message with the corresponding value for this rule, gotten from the excel sheet.
    message="This child is showing more than one open Child Protection plan, i.e. with no End Date",
    # The column names tend to be the words within the < > signs in the github issue description.
    affected_fields=[CPPendDate],
)
def validate(
    data_container: Mapping[CINTable, pd.DataFrame], rule_context: RuleContext
):
    # PREPARING DATA

    # Replace ChildProtectionPlans with the name of the table you need.
    df = data_container[ChildProtectionPlans]
    # Before you begin, rename the index and make it a column, so that the initial row positions can be kept intact.
    df.index.name = "ROW_ID"
    df.reset_index(inplace=True)

    # lOGIC
    # Implement rule logic as described by the Github issue.
    # Put the description as a comment above the implementation as shown.

    # There must be only one <ChildProtectionPlans> group where the <CPPendDate> (N00115) is missing

    # DF_CHECK: APPLY GROUPBYs IN A SEPARATE DATAFRAME SO THAT OTHER COLUMNS ARE NOT LOST OR CORRUPTED. THEN, MAP THE RESULTS TO THE INITIAL DATAFRAME.
    df_check = df.copy()
    # get all the locations where CPPendDate is null
    df_check = df_check[df_check[CPPendDate].isna()]
    # We'll have to count the number of nan values per group. NaNs cannot be counted so replace them with something that can.
    # Do this only if your rule requires that you interact with a column made up of all NaNs.
    df_check[CPPendDate].fillna(1, inplace=True)
    # count how many occurences of missing CPPendDate per ChildProtectionPlan group in each child.
    df_check = df_check.groupby([LAchildID, CPPID])[CPPendDate].count().reset_index()

    # when you groupby as shown above a series is returned where the columns in the round brackets become the index and the groupby result are the values.
    # resetting the index pushes the columns in the () back as columns of the dataframe and assigns the groupby result to the column in the square bracket.

    # filter out the instances where CPPendDate is missing more than once in a CP plan group.
    df_check = df_check[df_check[CPPendDate] > 1]
    issue_ids = tuple(zip(df_check[LAchildID], df_check[CPPID]))

    # DF_ISSUES: GET ALL THE DATA ABOUT THE LOCATIONS THAT WERE IDENTIFIED IN DF_CHECK
    df["ERROR_ID"] = tuple(zip(df[LAchildID], df[CPPID]))
    df_issues = df[df.ERROR_ID.isin(issue_ids)]

    df_issues = (
        df_issues.groupby("ERROR_ID", group_keys=False)["ROW_ID"]
        .apply(list)
        .reset_index()
    )
    # Ensure that you do not change the ROW_ID, and ERROR_ID column names which are shown above. They are keywords in this project.
    rule_context.push_type_3(
        table=ChildProtectionPlans, columns=[CPPendDate], row_df=df_issues
    )


def test_validate():
    # Create some sample data such that some values pass the validation and some fail.
    sample_ChildProtectionPlans = pd.DataFrame(
        [  # child1
            {  # fail
                LAchildID: "child1",
                CPPID: "CPPID1",
                CPPendDate: pd.NA,  # 0 first nan date in group
            },
            {  # fail
                LAchildID: "child1",
                CPPID: "CPPID1",
                CPPendDate: pd.NA,  # 1 second nan date in group
            },
            {  # won't be flagged because there is not more than one nan authorisation date in this group.
                LAchildID: "child1",
                CPPID: "CPPID2",
                CPPendDate: pd.NA,  # 2
            },
            # child2
            {
                LAchildID: "child2",
                CPPID: "CPPID1",
                CPPendDate: "26/05/2021",  # 3 ignored. not nan
            },
            {  # fail
                LAchildID: "child2",
                CPPID: "CPPID2",
                CPPendDate: pd.NA,  # 4 first nan date in group
            },
            {  # fail
                LAchildID: "child2",
                CPPID: "CPPID2",
                CPPendDate: pd.NA,  # 5 second nan date in group
            },
        ]
    )
    # if rule requires columns containing date values, convert those columns to
    # datetime objects first. Do it here in the test_validate function, not above.
    sample_ChildProtectionPlans[CPPendDate] = pd.to_datetime(
        sample_ChildProtectionPlans[CPPendDate],
        format="%d/%m/%Y",
        errors="coerce",
    )

    # Run rule function passing in our sample data
    result = run_rule(validate, {ChildProtectionPlans: sample_ChildProtectionPlans})

    # Use .type3_issues to check for the result of .push_type3_issues() which you used above.
    issues_list = result.type3_issues
    # Issues list contains the objects pushed in their respective order. Since push_type3 was only used once, there will be one object in issues_list.
    assert len(issues_list) == 1
    issues = issues_list[0]

    # get table name and check it. Replace ChildProtectionPlans with the name of your table.
    issue_table = issues.table
    assert issue_table == ChildProtectionPlans

    # check that the right columns were returned. Replace CPPendDate with a list of your columns.
    issue_columns = issues.columns
    assert issue_columns == [CPPendDate]

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
                    "CPPID1",
                ),
                "ROW_ID": [0, 1],
            },
            {
                "ERROR_ID": (
                    "child2",
                    "CPPID2",
                ),
                "ROW_ID": [4, 5],
            },
        ]
    )
    assert issue_rows.equals(expected_df)

    # Check that the rule definition is what you wrote in the context above.

    # replace 8925 with the rule code and put the appropriate message in its place too.
    assert result.definition.code == "8935"
    assert (
        result.definition.message
        == "This child is showing more than one open Child Protection plan, i.e. with no End Date"
    )
