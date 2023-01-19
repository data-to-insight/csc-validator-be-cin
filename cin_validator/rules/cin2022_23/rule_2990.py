from typing import Mapping

import pandas as pd

from cin_validator.rule_engine import CINTable, RuleContext, rule_definition
from cin_validator.test_engine import run_rule
from cin_validator.utils import make_census_period

# Get tables and columns of interest from the CINTable object defined in rule_engine/__api.py

ChildProtectionPlans = CINTable.ChildProtectionPlans
CINdetails = CINTable.CINdetails
Section47 = CINTable.Section47
CINplanDates = CINTable.CINplanDates

ReasonForClosure = CINdetails.ReasonForClosure
LAchildID = ChildProtectionPlans.LAchildID
CINdetailsID = ChildProtectionPlans.CINdetailsID
DateOfInitialCPC = CINdetails.DateOfInitialCPC

# define characteristics of rule
@rule_definition(
    # write the rule code here, in place of 2885
    code=2990,
    # replace CINdetails with the value in the module column of the excel sheet corresponding to this rule .
    # Note that even if multiple tables are involved, one table will be named in the module column.
    module=CINTable.CINdetails,
    # replace the message with the corresponding value for this rule, gotten from the excel sheet.
    message="Activity is recorded against a case marked as ‘Case closed after assessment, no further action’.",
    # The column names tend to be the words within the < > signs in the github issue description.
    affected_fields=[
        ReasonForClosure,
    ],
)
def validate(
    data_container: Mapping[CINTable, pd.DataFrame], rule_context: RuleContext
):
    # PREPARING DATA

    # Replace ChildProtectionPlans with the name of the table you need.
    df_cpp = data_container[ChildProtectionPlans].copy()
    df_47 = data_container[Section47].copy()
    df_cin = data_container[CINdetails].copy()
    df_cin_pd = data_container[CINplanDates].copy()

    # Before you begin, rename the index so that the initial row positions can be kept intact.
    df_cpp.index.name = "ROW_ID"
    df_47.index.name = "ROW_ID"
    df_cin.index.name = "ROW_ID"
    df_cin_pd.index.name = "ROW_ID"

    # Resetting the index causes the ROW_IDs to become columns of their respective DataFrames
    # so that they can come along when the merge is done.
    # TODO summarise with a for loop? e.g for df in [df_cpp, df_47, df_cin]
    df_cpp.reset_index(inplace=True)
    df_47.reset_index(inplace=True)
    df_cin.reset_index(inplace=True)
    df_cin_pd.reset_index(inplace=True)

    # If a <CINDetails> module has <ReasonForClosure> (N00103) = RC8, then it cannot have any of the following modules:
    # <Section47> module
    # <ChildProtectionPlan> module
    # <DateofInitialCPC> (N00110) within the <CINDetails> module
    # <CINPlanDates> module
    df_cin = df_cin[df_cin[ReasonForClosure] == "RC8"]

    df_cin_cpp = df_cin.merge(
        df_cpp, on=["LAchildID", "CINdetailsID"], how="left", suffixes=["_cin", "_cpp"]
    )

    df_cin_47 = df_cin.merge(
        df_47,
        on=["LAchildID", "CINdetailsID", DateOfInitialCPC],
        how="left",
        suffixes=["_cin", "_47"],
    )

    df_cin_cin_pd = df_cin.merge(
        df_cin_pd,
        on=["LAchildID", "CINdetailsID"],
        how="left",
        suffixes=["_cin", "_pd"],
    )

    df_cin_cpp_47 = df_cin_cpp.merge(
        df_cin_47,
        on=[
            "LAchildID",
            "CINdetailsID",
            "ROW_ID_cin",
            "DateOfInitialCPC",
            "ReasonForClosure",
        ],
        how="left",
        suffixes=["_cin_cpp", "_cin_47"],
    )
    merged_df = df_cin_cpp_47.merge(
        df_cin_cin_pd,
        on=[
            "LAchildID",
            "CINdetailsID",
            "ROW_ID_cin",
            "DateOfInitialCPC",
            "ReasonForClosure",
        ],
        how="left",
        suffixes=["_cin_cpp_47", "_cin_cin_pd"],
    )

    # Logical conditions - other than this, of the tables can merge, it means there's modules and they are in error

    condition_1 = merged_df[DateOfInitialCPC].notna()
    condition_2 = merged_df["ROW_ID_cpp"].notna()
    condition_3 = merged_df["ROW_ID_47"].notna()
    condition_4 = merged_df["ROW_ID_pd"].notna()

    merged_df = merged_df[
        condition_1 | condition_2 | condition_3 | condition_4
    ].reset_index()

    # create an identifier for each error instance.
    # In this case, the rule is checked for each CPPstartDate, in each CINplanDates group (differentiated by CINdetailsID), in each child (differentiated by LAchildID)
    # So, a combination of LAchildID, CINdetailsID and CPPstartDate identifies and error instance.
    # You could also consider that CPPstartDate, unlike DateOfInitialCPC, is the leading column against which columns from the other tables are compared. So it is included in the zip.
    merged_df["ERROR_ID"] = tuple(zip(merged_df[LAchildID], merged_df[CINdetailsID]))

    # The merges were done on copies of df_cpp, df_47 and df_cin so that the column names in dataframes themselves aren't affected by the suffixes.
    # we can now map the suffixes columns to their corresponding source tables such that the failing ROW_IDs and ERROR_IDs exist per table.
    df_cpp_issues = (
        df_cpp.merge(merged_df, left_on="ROW_ID", right_on="ROW_ID_cpp")
        .groupby("ERROR_ID", group_keys=False)["ROW_ID"]
        .apply(list)
        .reset_index()
    )
    df_47_issues = (
        df_47.merge(merged_df, left_on="ROW_ID", right_on="ROW_ID_47")
        .groupby("ERROR_ID", group_keys=False)["ROW_ID"]
        .apply(list)
        .reset_index()
    )
    df_cin_issues = (
        df_cin.merge(merged_df, left_on="ROW_ID", right_on="ROW_ID_cin")
        .groupby("ERROR_ID", group_keys=False)["ROW_ID"]
        .apply(list)
        .reset_index()
    )

    df_cin_pd_issues = (
        df_cin_pd.merge(merged_df, left_on="ROW_ID", right_on="ROW_ID_pd")
        .groupby("ERROR_ID", group_keys=False)["ROW_ID"]
        .apply(list)
        .reset_index()
    )
    # Ensure that you maintain the ROW_ID, and ERROR_ID column names which are shown above. They are keywords in this project.
    rule_context.push_type_2(
        table=ChildProtectionPlans, columns=[LAchildID], row_df=df_cpp_issues
    )
    rule_context.push_type_2(
        table=CINdetails, columns=[DateOfInitialCPC], row_df=df_cin_issues
    )
    rule_context.push_type_2(
        table=CINplanDates, columns=[LAchildID], row_df=df_cin_pd_issues
    )
    rule_context.push_type_2(table=Section47, columns=[LAchildID], row_df=df_47_issues)


def test_validate():
    # Create some sample data such that some values pass the validation and some fail.
    sample_cpp = pd.DataFrame(
        [
            {
                "LAchildID": "child1",
                "CPPstartDate": "26/05/2000",
                "CINdetailsID": "cinID10",
            },
            {
                "LAchildID": "child1",
                "CPPstartDate": "27/06/2002",
                "CINdetailsID": "cinID2",
            },
            {  # fails for having a module
                "LAchildID": "child3",
                "CPPstartDate": "26/05/2000",
                "CINdetailsID": "cinID3",
            },
            {
                "LAchildID": "child30",
                "CPPstartDate": "26/05/2000",
                "CINdetailsID": "cinID3",
            },
            {
                "LAchildID": "child30",
                "CPPstartDate": pd.NA,
                "CINdetailsID": "cinID2",
            },
            {
                "LAchildID": "child3",
                "CPPstartDate": "07/02/2001",
                "CINdetailsID": "cinID30",
            },
            {
                "LAchildID": "child3",
                "CPPstartDate": "14/03/2001",
                "CINdetailsID": "cinID4",
            },
        ]
    )
    sample_section47 = pd.DataFrame(
        [
            {
                "LAchildID": "child1",
                "DateOfInitialCPC": pd.NA,
                "CINdetailsID": "cinID2",
            },
            {
                "LAchildID": "child2",  # fails for having a module
                "DateOfInitialCPC": pd.NA,
                "CINdetailsID": "cinID2",
            },
            {
                "LAchildID": "child2",
                "DateOfInitialCPC": pd.NA,
                "CINdetailsID": "cinID1",
            },
            {
                "LAchildID": "child3",
                "DateOfInitialCPC": pd.NA,
                "CINdetailsID": "cinID1",
            },
            {
                "LAchildID": "child3",
                "DateOfInitialCPC": "26/05/2000",
                "CINdetailsID": "cinID2",
            },
            {
                "LAchildID": "child3",
                "DateOfInitialCPC": pd.NA,
                "CINdetailsID": "cinID3",
            },
            {
                "LAchildID": "child3",
                "DateOfInitialCPC": "14/03/2001",
                "CINdetailsID": "cinID4",
            },
        ]
    )
    sample_cin_details = pd.DataFrame(
        [
            {
                "LAchildID": "child1",
                "DateOfInitialCPC": pd.NA,  # 0 fail: found in CINplanDates table
                "CINdetailsID": "cinID1",
                "ReasonForClosure": "RC8",
            },
            {
                "LAchildID": "child2",
                "DateOfInitialCPC": pd.NA,
                "CINdetailsID": "cinID2",  # 1 fail: found in Section47 table
                "ReasonForClosure": "RC8",
            },
            {
                "LAchildID": "child3",
                "DateOfInitialCPC": pd.NA,  # 2 fail : found in Section47 and CPP tables
                "CINdetailsID": "cinID3",
                "ReasonForClosure": "RC8",
            },
            {
                "LAchildID": "child4",
                "DateOfInitialCPC": "28/05/2000",  # 3 fails for having initialcpc
                "CINdetailsID": "cinID4",
                "ReasonForClosure": "RC8",
            },
            {
                "LAchildID": "child3",
                "DateOfInitialCPC": "26/05/2000",
                "CINdetailsID": "cinID2",
                "ReasonForClosure": "RC9",  # 4 ignore: reason != RC8
            },
            {
                "LAchildID": "child3",
                "DateOfInitialCPC": "26/05/2003",
                "CINdetailsID": "cinID8",
                "ReasonForClosure": "RC9",  # 5 ignore: reason != RC8
            },
            {
                "LAchildID": "child3",
                "DateOfInitialCPC": "14/03/2001",
                "CINdetailsID": "cinID4",
                "ReasonForClosure": "RC9",  # 6 ignore: reason != RC8
            },
            {
                "LAchildID": "child7",
                "DateOfInitialCPC": pd.NA,
                "CINdetailsID": "cinID4",
                "ReasonForClosure": "RC9",  # 7 pass
            },
        ]
    )
    sample_cin_plan_dates = pd.DataFrame(
        [
            {
                "LAchildID": "child1",  # fails for having a module
                "CINdetailsID": "cinID1",
            },
            {
                "LAchildID": "child1",
                "CINdetailsID": "cinID2",
            },
            {
                "LAchildID": "child1",
                "CINdetailsID": "cinID3",
            },
            {
                "LAchildID": "child1",
                "CINdetailsID": "cinID4",
            },
        ]
    )
    # if rule requires columns containing date values, convert those columns to datetime objects first. Do it here in the test_validate function, not above.

    sample_cin_details["DateOfInitialCPC"] = pd.to_datetime(
        sample_cin_details["DateOfInitialCPC"], format="%d/%m/%Y", errors="coerce"
    )
    sample_section47["DateOfInitialCPC"] = pd.to_datetime(
        sample_section47["DateOfInitialCPC"], format="%d/%m/%Y", errors="coerce"
    )

    # Run the rule function, passing in our sample data.
    result = run_rule(
        validate,
        {
            ChildProtectionPlans: sample_cpp,
            Section47: sample_section47,
            CINdetails: sample_cin_details,
            CINplanDates: sample_cin_plan_dates,
        },
    )

    # Use .type2_issues to check for the result of .push_type2_issues() which you used above.
    issues_list = result.type2_issues
    assert len(issues_list) == 4
    # the function returns a list on NamedTuples where each NamedTuple contains (table, column_list, df_issues)
    # pick any table and check it's values. the tuple in location 1 will contain the Section47 columns because that's the second thing pushed above.
    issues = issues_list[1]

    # get table name and check it. Replace Section47 with the name of your table.
    issue_table = issues.table
    assert issue_table == CINdetails

    # check that the right columns were returned. Replace DateOfInitialCPC  with a list of your columns.
    issue_columns = issues.columns
    assert issue_columns == [DateOfInitialCPC]

    # check that the location linking dataframe was formed properly.
    issue_rows = issues.row_df
    # replace 2 with the number of failing points you expect from the sample data.
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
                    "child1",  # ChildID
                    "cinID1",  # CINdetailsID
                ),
                "ROW_ID": [0],
            },
            {
                "ERROR_ID": (
                    "child2",  # ChildID
                    "cinID2",  # CINdetailsID
                ),
                "ROW_ID": [1],
            },
            {
                "ERROR_ID": (
                    "child3",  # ChildID
                    "cinID3",  # CINdetailsID
                ),
                "ROW_ID": [2],
            },
            {
                "ERROR_ID": (
                    "child4",  # ChildID
                    "cinID4",  # CINdetailsID
                ),
                "ROW_ID": [3],
            },
        ]
    )
    assert issue_rows.equals(expected_df)

    # Check that the rule definition is what you wrote in the context above.

    # replace 2885 with the rule code and put the appropriate message in its place too.
    assert result.definition.code == 2990
    assert (
        result.definition.message
        == "Activity is recorded against a case marked as ‘Case closed after assessment, no further action’."
    )
