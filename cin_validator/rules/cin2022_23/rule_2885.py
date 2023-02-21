from typing import Mapping

import pandas as pd

from cin_validator.rule_engine import CINTable, RuleContext, rule_definition
from cin_validator.test_engine import run_rule
from cin_validator.utils import make_census_period

ChildProtectionPlans = CINTable.ChildProtectionPlans
CINdetails = CINTable.CINdetails
Section47 = CINTable.Section47

CPPstartDate = ChildProtectionPlans.CPPstartDate
# TODO is it necessary to get LAchildID per table involved?
LAchildID = ChildProtectionPlans.LAchildID
CINdetailsID = ChildProtectionPlans.CINdetailsID
# TODO is it redundant to have both of them here?
DateOfInitialCPC = Section47.DateOfInitialCPC
DateOfInitialCPC = CINdetails.DateOfInitialCPC

# Reference date in header is needed to define the period of census.
Header = CINTable.Header
ReferenceDate = Header.ReferenceDate

# define characteristics of rule
@rule_definition(
    # write the rule code here, in place of 2885
    code=2885,
    # replace ChildProtectionPlans with the value in the module column of the excel sheet corresponding to this rule .
    # Note that even if multiple tables are involved, one table will be named in the module column.
    module=CINTable.ChildProtectionPlans,
    # replace the message with the corresponding value for this rule, gotten from the excel sheet.
    message="Child protection plan shown as starting a different day to the initial child protection conference.",
    # The column names tend to be the words within the < > signs in the github issue description.
    affected_fields=[
        CPPstartDate,
        DateOfInitialCPC,
    ],  # TODO How can we indicate that the DateOfInitialCPC comes from both tables. Is it necessary?
)
def validate(
    data_container: Mapping[CINTable, pd.DataFrame], rule_context: RuleContext
):
    # PREPARING DATA

    # Replace ChildProtectionPlans with the name of the table you need.
    df_cpp = data_container[ChildProtectionPlans].copy()
    # TODO Is it necessary to copy? How else can we ensure that the original data is not changed in each rule.?.
    df_47 = data_container[Section47].copy()
    df_cin = data_container[CINdetails].copy()

    # Before you begin, rename the index so that the initial row positions can be kept intact.
    df_cpp.index.name = "ROW_ID"
    df_47.index.name = "ROW_ID"
    df_cin.index.name = "ROW_ID"

    # Resetting the index causes the ROW_IDs to become columns of their respective DataFrames
    # so that they can come along when the merge is done.
    # TODO summarise with a for loop? e.g for df in [df_cpp, df_47, df_cin]
    df_cpp.reset_index(inplace=True)
    df_47.reset_index(inplace=True)
    df_cin.reset_index(inplace=True)

    # get collection period
    header = data_container[Header]
    ref_date_series = header[ReferenceDate]
    collection_start, collection_end = make_census_period(ref_date_series)

    # lOGIC
    # Implement rule logic as described by the Github issue.
    # Put the description as a comment above the implementation as shown.

    # If present and if <CPPStartDate> (N00105) falls within [Period_Of_Census], then the <CPPStartDate> (N00105) should equal either:
    # a) a Section47 module <DateOfInitialCPC> (N00110), or
    # b) a CINDetails module <DateOfInitialCPC> (N00110) if there is no associated Section 47 record.
    start_date_present = df_cpp[CPPstartDate].notna()
    within_period = (df_cpp[CPPstartDate] >= collection_start) & (
        df_cpp[CPPstartDate] <= collection_end
    )
    df_cpp = df_cpp[start_date_present & within_period]

    # left merge means that only the filtered cpp children will be considered and there is no possibility of additonal children coming in from other tables.

    # get only the section47 rows where cppstartdate exists and is within period.
    df_cpp_47 = df_cpp.copy().merge(
        df_47.copy(), on=[LAchildID, CINdetailsID], how="left", suffixes=["_cpp", "_47"]
    )
    # get only the cindetails rows where cppstartdate exists and is within period.
    df_cpp_cin = df_cpp.copy().merge(
        df_cin.copy(),
        on=[LAchildID, CINdetailsID],
        how="left",
        suffixes=["_cpp", "_cin"],
    )
    # Create a dataframe that contains all the cin_details and cpp information for children whose cppstartdate exists and is within period.
    merged_df = df_cpp_47.merge(
        df_cpp_cin,
        # since the cpp columns are common in both dfs, merge on them. TODO: is this okay?
        on=[LAchildID, CPPstartDate, "CINdetailsID", "ROW_ID_cpp"],
        suffixes=["_47", "_cin"],
        # the suffixes apply to all the columns not "merged on". That is, DateOfInitialCPC
    )

    #  Filter out rows where there are multiple s47 modules, some with DateOfInitialCPC and some without
    no_dates = merged_df[
        merged_df["DateOfInitialCPC_47"].isna()
        & merged_df["DateOfInitialCPC_cin"].isna()
    ]["LAchildID"].to_list()
    at_least_one_date = merged_df[
        merged_df["DateOfInitialCPC_47"].notna()
        | merged_df["DateOfInitialCPC_cin"].notna()
    ]["LAchildID"].to_list()
    some_and_none = merged_df[
        merged_df[LAchildID].isin(at_least_one_date)
        & merged_df[LAchildID].isin(no_dates)
    ]["LAchildID"].to_list()

    # This filters out children who have some rows with dates and some without in the same CINdetailsID module
    # It takes out empty rows, but leaves in rows with dates. If the rows with dates pass, the child passes as it should,
    #  If the rows with dates fail, the child still fails as it should.
    # This is tested with chidlren 5 and 6 in the samples DFs, and was necessary to account for real world data.
    merged_df = merged_df[
        ~(
            (
                merged_df["DateOfInitialCPC_47"].isna()
                & merged_df["DateOfInitialCPC_cin"].isna()
            )
            & (merged_df[LAchildID].isin(some_and_none))
        )
    ]

    # check that the the dates being compared existed in the same CIN event period and belong to the same child.
    # This checks if there are any modules which fail, even if there's another module that passes which should pass the child
    condition = (merged_df[CPPstartDate] != merged_df["DateOfInitialCPC_47"]) & (
        merged_df[CPPstartDate] != merged_df["DateOfInitialCPC_cin"]
    )

    # If the rule means that CINdetails' value should also be checked *only if* Section47's value is NaN, then
    # condition = (merged_df[CPPstartDate] != merged_df[DateOfInitialCPC_47]) & merged_df[DateOfInitialCPC_47].notna()
    #                | (merged_df[DateOfInitialCPC_47].isna()&(merged_df[CPPstartDate] != merged_df[DateOfInitialCPC_cin]))

    # get all the data that fits the failing condition.
    merged_df = merged_df[condition].reset_index()

    # create an identifier for each error instance.
    # In this case, the rule is checked for each CPPstartDate, in each CINplanDates group (differentiated by CINdetailsID), in each child (differentiated by LAchildID)
    # So, a combination of LAchildID, CINdetailsID and CPPstartDate identifies and error instance.
    # You could also consider that CPPstartDate, unlike DateOfInitialCPC, is the leading column against which columns from the other tables are compared. So it is included in the zip.
    merged_df["ERROR_ID"] = tuple(
        zip(merged_df[LAchildID], merged_df[CINdetailsID], merged_df[CPPstartDate])
    )

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
        df_cin.merge(merged_df, left_on="ROW_ID", right_on="ROW_ID_cpp")
        .groupby("ERROR_ID", group_keys=False)["ROW_ID"]
        .apply(list)
        .reset_index()
    )

    # Ensure that you maintain the ROW_ID, and ERROR_ID column names which are shown above. They are keywords in this project.
    rule_context.push_type_2(
        table=ChildProtectionPlans, columns=[CPPstartDate], row_df=df_cpp_issues
    )
    rule_context.push_type_2(
        table=Section47, columns=[DateOfInitialCPC], row_df=df_47_issues
    )
    rule_context.push_type_2(
        table=CINdetails, columns=[DateOfInitialCPC], row_df=df_cin_issues
    )


def test_validate():
    # Create some sample data such that some values pass the validation and some fail.
    sample_cpp = pd.DataFrame(
        [
            {  # same as Section47 date, different from cin date
                "LAchildID": "child1",
                "CPPstartDate": "26/05/2021",  # 0 pass
                "CINdetailsID": "cinID1",
            },
            {  # would've failed but ignored. Not in period of census
                "LAchildID": "child1",
                "CPPstartDate": "27/06/2002",  # 1 ignored
                "CINdetailsID": "cinID2",
            },
            {  # same as cin_date, different from section47
                "LAchildID": "child2",
                "CPPstartDate": "26/05/2021",  # 2 pass [Should fail if other condition is used and section47 is present]
                "CINdetailsID": "cinID1",
            },
            {  # different from both dates
                "LAchildID": "child3",
                "CPPstartDate": "26/05/2021",  # 3 fail
                "CINdetailsID": "cinID1",
            },
            {  # absent
                "LAchildID": "child3",
                "CPPstartDate": pd.NA,  # 4 ignore
                "CINdetailsID": "cinID2",
            },
            {  # fail
                "LAchildID": "child3",
                "CPPstartDate": "07/02/2022",  # 5 fail. Different from both cin_dates in its cindetails group
                "CINdetailsID": "cinID3",
            },
            {  # section47 date is absent, same as cin date.
                # If grouping is not done well, this date could cause (LAchildID3, CINdetailsID3) above to pass.
                "LAchildID": "child3",
                "CPPstartDate": "14/03/2022",  # 6 pass [Should fail if other condition is used]
                "CINdetailsID": "cinID4",
            },
            {
                "LAchildID": "child5",
                "CPPstartDate": "19/07/2021",
                "CINdetailsID": "cinID4",
            },
            {
                "LAchildID": "child6",
                "CPPstartDate": "19/07/2021",
                "CINdetailsID": "cinID4",
            },
        ]
    )
    sample_section47 = pd.DataFrame(
        [
            {  # 0 pass
                "LAchildID": "child1",
                "DateOfInitialCPC": "26/05/2021",
                "CINdetailsID": "cinID1",
            },
            {  # 1 ignored
                "LAchildID": "child1",
                "DateOfInitialCPC": "26/05/2021",
                "CINdetailsID": "cinID2",
            },
            {  # 2 pass
                "LAchildID": "child2",
                "DateOfInitialCPC": "30/05/2021",
                "CINdetailsID": "cinID1",
            },
            {  # 3 fail
                "LAchildID": "child3",
                "DateOfInitialCPC": "27/05/2021",
                "CINdetailsID": "cinID1",
            },
            {  # 4 absent, ignored
                "LAchildID": "child3",
                "DateOfInitialCPC": "26/05/2021",
                "CINdetailsID": "cinID2",
            },
            {  # 5 fail
                "LAchildID": "child3",
                "DateOfInitialCPC": "26/05/2021",
                "CINdetailsID": "cinID3",
            },
            {  # 6 pass
                "LAchildID": "child3",
                "DateOfInitialCPC": pd.NA,
                "CINdetailsID": "cinID4",
            },
            {
                "LAchildID": "child5",
                "DateOfInitialCPC": "19/07/2021",
                "CINdetailsID": "cinID4",
            },
            {
                "LAchildID": "child5",
                "DateOfInitialCPC": pd.NA,
                "CINdetailsID": "cinID4",
            },
            {
                "LAchildID": "child6",
                "DateOfInitialCPC": pd.NA,
                "CINdetailsID": "cinID4",
            },
        ]
    )
    sample_cin_details = pd.DataFrame(
        [
            {  # 0 pass
                "LAchildID": "child1",
                "DateOfInitialCPC": "26/10/2020",
                "CINdetailsID": "cinID1",
            },
            {  # 1 ignore
                "LAchildID": "child1",
                "DateOfInitialCPC": "26/05/2021",
                "CINdetailsID": "cinID2",
            },
            {  # 2 pass
                "LAchildID": "child2",
                "DateOfInitialCPC": "26/05/2021",
                "CINdetailsID": "cinID1",
            },
            {  # 3 fail
                "LAchildID": "child3",
                "DateOfInitialCPC": "28/05/2021",
                "CINdetailsID": "cinID1",
            },
            {  # 4 ignore
                "LAchildID": "child3",
                "DateOfInitialCPC": "26/05/2021",
                "CINdetailsID": "cinID2",
            },
            {  # 5 fail
                "LAchildID": "child3",
                "DateOfInitialCPC": "26/05/2003",
                "CINdetailsID": "cinID3",
            },
            {  # 6 pass
                "LAchildID": "child3",
                "DateOfInitialCPC": "14/03/2022",
                "CINdetailsID": "cinID4",
            },
            {
                "LAchildID": "child5",
                "DateOfInitialCPC": pd.NA,
                "CINdetailsID": "cinID4",
            },
            {
                "LAchildID": "child6",
                "DateOfInitialCPC": pd.NA,
                "CINdetailsID": "cinID4",
            },
        ]
    )
    # if rule requires columns containing date values, convert those columns to datetime objects first. Do it here in the test_validate function, not above.
    sample_header = pd.DataFrame(
        [{ReferenceDate: "31/03/2022"}]  # the census start date here will be 01/04/2021
    )
    sample_header[ReferenceDate] = pd.to_datetime(
        sample_header[ReferenceDate], format="%d/%m/%Y", errors="coerce"
    )
    sample_cpp[CPPstartDate] = pd.to_datetime(
        sample_cpp[CPPstartDate], format="%d/%m/%Y", errors="coerce"
    )
    sample_section47["DateOfInitialCPC"] = pd.to_datetime(
        sample_section47["DateOfInitialCPC"], format="%d/%m/%Y", errors="coerce"
    )
    sample_cin_details["DateOfInitialCPC"] = pd.to_datetime(
        sample_cin_details["DateOfInitialCPC"], format="%d/%m/%Y", errors="coerce"
    )

    # Run the rule function, passing in our sample data.
    result = run_rule(
        validate,
        {
            ChildProtectionPlans: sample_cpp,
            Section47: sample_section47,
            CINdetails: sample_cin_details,
            Header: sample_header,
        },
    )

    # Use .type2_issues to check for the result of .push_type2_issues() which you used above.
    issues_list = result.type2_issues
    assert len(issues_list) == 3
    # the function returns a list on NamedTuples where each NamedTuple contains (table, column_list, df_issues)
    # pick any table and check it's values. the tuple in location 1 will contain the Section47 columns because that's the second thing pushed above.
    issues = issues_list[1]

    # get table name and check it. Replace Section47 with the name of your table.
    issue_table = issues.table
    assert issue_table == Section47

    # check that the right columns were returned. Replace DateOfInitialCPC  with a list of your columns.
    issue_columns = issues.columns
    assert issue_columns == [DateOfInitialCPC]

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
                    "child3",  # ChildID
                    "cinID1",  # CINdetailsID
                    # corresponding CPPstartDate
                    pd.to_datetime("26/05/2021", format="%d/%m/%Y", errors="coerce"),
                ),
                "ROW_ID": [3],
            },
            {
                "ERROR_ID": (
                    "child3",
                    "cinID3",
                    pd.to_datetime("07/02/2022", format="%d/%m/%Y", errors="coerce"),
                ),
                "ROW_ID": [5],
            },
            {
                "ERROR_ID": (
                    "child6",
                    "cinID4",
                    pd.to_datetime("19/07/2021", format="%d/%m/%Y", errors="coerce"),
                ),
                "ROW_ID": [9],
            },
        ]
    )
    assert issue_rows.equals(expected_df)

    # Check that the rule definition is what you wrote in the context above.

    # replace 2885 with the rule code and put the appropriate message in its place too.
    assert result.definition.code == 2885
    assert (
        result.definition.message
        == "Child protection plan shown as starting a different day to the initial child protection conference."
    )
