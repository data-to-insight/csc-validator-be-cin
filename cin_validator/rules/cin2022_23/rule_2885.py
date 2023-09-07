from typing import Mapping

import pandas as pd

from cin_validator.rule_engine import CINTable, RuleContext, rule_definition
from cin_validator.test_engine import run_rule
from cin_validator.utils import make_census_period

ChildProtectionPlans = CINTable.ChildProtectionPlans
CINdetails = CINTable.CINdetails
Section47 = CINTable.Section47

CPPstartDate = ChildProtectionPlans.CPPstartDate
LAchildID = ChildProtectionPlans.LAchildID
CINdetailsID = ChildProtectionPlans.CINdetailsID
DateOfInitialCPC = Section47.DateOfInitialCPC

# Reference date in header is needed to define the period of census.
Header = CINTable.Header
ReferenceDate = Header.ReferenceDate


# define characteristics of rule
@rule_definition(
    # write the rule code here, in place of '2885'
    code="2885",
    # replace ChildProtectionPlans with the value in the module column of the excel sheet corresponding to this rule .
    # Note that even if multiple tables are involved, one table will be named in the module column.
    module=CINTable.ChildProtectionPlans,
    # replace the message with the corresponding value for this rule, gotten from the excel sheet.
    message="Child protection plan shown as starting a different day to the initial child protection conference.",
    # The column names tend to be the words within the < > signs in the github issue description.
    affected_fields=[
        CPPstartDate,
        DateOfInitialCPC,
    ],
)
def validate(
    data_container: Mapping[CINTable, pd.DataFrame], rule_context: RuleContext
):
    # PREPARING DATA

    # Replace ChildProtectionPlans with the name of the table you need.
    df_cpp = data_container[ChildProtectionPlans]
    df_47 = data_container[Section47]
    df_cin = data_container[CINdetails]

    # Before you begin, rename the index so that the initial row positions can be kept intact.
    df_cpp.index.name = "ROW_ID"
    df_47.index.name = "ROW_ID"
    df_cin.index.name = "ROW_ID"

    # Resetting the index causes the ROW_IDs to become columns of their respective DataFrames
    # so that they can come along when the merge is done.

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
    df_cpp_47 = df_cpp.merge(
        df_47, on=[LAchildID, CINdetailsID], how="inner", suffixes=["_cpp", "_47"]
    )

    # FIND LOCATIONS THAT FAIL THE RULE

    # Section47 table: if CPPstartDate matches any DateOfInitialCPC within its CIN module, all DateOfInitialCPCs should pass in that CIN module.

    # locations with equal dates surely pass.
    df_cpp_47_pass = df_cpp_47[df_cpp_47[CPPstartDate] == df_cpp_47[DateOfInitialCPC]]
    # locations with unequal dates could fail.
    df_cpp_47_failable = df_cpp_47[
        df_cpp_47[CPPstartDate] != df_cpp_47[DateOfInitialCPC]
    ]
    # the failable locations that are not found in a CIN module where a DateOfInitialCPC passes, surely fail.
    df_cpp_47_failable["ERROR_ID"] = tuple(
        zip(df_cpp_47_failable[LAchildID], df_cpp_47_failable[CINdetailsID])
    )
    df_cpp_47_pass["ERROR_ID"] = tuple(
        zip(df_cpp_47_pass[LAchildID], df_cpp_47_pass[CINdetailsID])
    )
    df_cpp_47_fail = df_cpp_47_failable[
        ~(df_cpp_47_failable["ERROR_ID"].isin(df_cpp_47_pass["ERROR_ID"]))
    ]

    # One CPPstartDate's pass should not affect the other. Create a separate reference dataset for CPPstartDate failing locations.
    # Redo the above steps to create a dataset where a CPPstartDate is only removed from failable if its LAchildID-CINdetails-CPPstartDate is found to pass.
    df_cpp_47_failable["ERROR_startdate"] = tuple(
        zip(
            df_cpp_47_failable[LAchildID],
            df_cpp_47_failable[CINdetailsID],
            df_cpp_47_failable[CPPstartDate],
        )
    )
    df_cpp_47_pass["ERROR_startdate"] = tuple(
        zip(
            df_cpp_47_pass[LAchildID],
            df_cpp_47_pass[CINdetailsID],
            df_cpp_47_pass[CPPstartDate],
        )
    )
    df_cpp_47_startdate_fail = df_cpp_47_failable[
        ~(df_cpp_47_failable["ERROR_startdate"].isin(df_cpp_47_pass["ERROR_startdate"]))
    ]

    # CIN table: if CPPstartDate matches any DateOfInitialCPC within its CIN module, all DateOfInitialCPCs should pass in that CIN module.

    df_cpp_cin = df_cpp.merge(
        df_cin, on=[LAchildID, CINdetailsID], how="left", suffixes=["_cpp", "_cin"]
    )
    df_cpp_cin_pass = df_cpp_cin[
        df_cpp_cin[CPPstartDate] == df_cpp_cin[DateOfInitialCPC]
    ]
    df_cpp_cin_failable = df_cpp_cin[
        df_cpp_cin[CPPstartDate] != df_cpp_cin[DateOfInitialCPC]
    ]

    df_cpp_cin_failable["ERROR_ID"] = tuple(
        zip(df_cpp_cin_failable[LAchildID], df_cpp_cin_failable[CINdetailsID])
    )
    df_cpp_cin_pass["ERROR_ID"] = tuple(
        zip(df_cpp_cin_pass[LAchildID], df_cpp_cin_pass[CINdetailsID])
    )
    df_cpp_cin_fail = df_cpp_cin_failable[
        ~(df_cpp_cin_failable["ERROR_ID"].isin(df_cpp_cin_pass["ERROR_ID"]))
    ]

    df_cpp_cin_failable["ERROR_startdate"] = tuple(
        zip(
            df_cpp_cin_failable[LAchildID],
            df_cpp_cin_failable[CINdetailsID],
            df_cpp_cin_failable[CPPstartDate],
        )
    )
    df_cpp_cin_pass["ERROR_startdate"] = tuple(
        zip(
            df_cpp_cin_pass[LAchildID],
            df_cpp_cin_pass[CINdetailsID],
            df_cpp_cin_pass[CPPstartDate],
        )
    )
    df_cpp_cin_startdate_fail = df_cpp_cin_failable[
        ~(
            df_cpp_cin_failable["ERROR_startdate"].isin(
                df_cpp_cin_pass["ERROR_startdate"]
            )
        )
    ]

    # Since the condition is to check the CIN table only if the LAchildID-CINdetailID is absent in Section47,
    # only those failing locations that are absent in section47 table should be flagged in the cindetails table. Locations that are flagged in section47 don't need to re-flag here.

    # create an ID that cin groups can be identified by.
    df_47["ERROR_ID"] = tuple(zip(df_47[LAchildID], df_47[CINdetailsID]))
    df_cpp_cin_fail_no_47 = df_cpp_cin_fail[
        ~(df_cpp_cin_fail["ERROR_ID"].isin(df_47["ERROR_ID"]))
    ]
    # do same for the cppstartdate identifier table.
    df_cpp_cin_startdate_fail_no47 = df_cpp_cin_startdate_fail[
        ~(df_cpp_cin_startdate_fail["ERROR_ID"].isin(df_47["ERROR_ID"]))
    ]

    # MAP FAILING LOCATIONS TO THEIR ORIGINAL TABLES.
    df_47_issues = df_47[df_47["ROW_ID"].isin(df_cpp_47_fail["ROW_ID_47"])]
    df_47_issues = (
        df_47_issues.groupby("ERROR_ID", group_keys=False)["ROW_ID"]
        .apply(list)
        .reset_index()
    )

    df_cin["ERROR_ID"] = tuple(zip(df_cin[LAchildID], df_cin[CINdetailsID]))
    df_cin_issues = df_cin[df_cin["ROW_ID"].isin(df_cpp_cin_fail_no_47["ROW_ID_cin"])]
    df_cin_issues = (
        df_cin_issues.groupby("ERROR_ID", group_keys=False)["ROW_ID"]
        .apply(list)
        .reset_index()
    )

    df_cpp["ERROR_ID"] = tuple(
        zip(df_cpp[LAchildID], df_cpp[CINdetailsID], df_cpp[CPPstartDate])
    )
    df_cpp_issues = df_cpp[
        (df_cpp["ROW_ID"].isin(df_cpp_47_startdate_fail["ROW_ID_cpp"]))
        | (df_cpp["ROW_ID"].isin(df_cpp_cin_startdate_fail_no47["ROW_ID_cpp"]))
    ]
    df_cpp_issues = (
        df_cpp_issues.groupby("ERROR_ID", group_keys=False)["ROW_ID"]
        .apply(list)
        .reset_index()
    )

    # In this case, the rule is checked for each CPPstartDate, in each CINplanDates group (differentiated by CINdetailsID), in each child (differentiated by LAchildID)
    # However, the cin and section47 locations fail/pass per group so LAchildID-CINdetailsID is used to identify them.
    # On the other hand, CPPstartDate locations fail/pass independently so a combination of LAchildID, CINdetailsID and CPPstartDate identifies a cpp error instance.

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
        [  # child1: Simulates multiple cin modules with one out of census period, and multiple CPPs within the same CIN.
            {
                "LAchildID": "child1",
                "CINdetailsID": "cinID1",
                "CPPstartDate": "26/05/2021",  #  passes in section47
            },
            {
                "LAchildID": "child1",
                "CINdetailsID": "cinID1",  # simulates multiple CPPs with same LAchildID-CINdetailsID where some fail, some pass.
                "CPPstartDate": "26/06/2021",  #  # fail. fails both section47 and cin
            },
            {
                "LAchildID": "child1",
                "CINdetailsID": "cinID2",
                "CPPstartDate": "27/06/2002",  # ignore. would've failed but ignored. Not in period of census
            },
            # child2: fail in section47, pass in cin
            {
                "LAchildID": "child2",
                "CINdetailsID": "cinID1",
                "CPPstartDate": "26/05/2021",  # fail. fails in section47, pass in cin not considered.
            },
            # child3: multiple cin details modules.
            {
                "LAchildID": "child3",
                "CINdetailsID": "cinID1",
                "CPPstartDate": "26/05/2021",  # fail. fails in both section47 and cin
            },
            {
                "LAchildID": "child3",
                "CINdetailsID": "cinID2",
                "CPPstartDate": pd.NA,  # ignore. cppstartdate is absent
            },
            {
                "LAchildID": "child3",
                "CINdetailsID": "cinID3",
                "CPPstartDate": "07/02/2022",  # fail. fails both section47 and cin
            },
            {
                "LAchildID": "child3",
                "CINdetailsID": "cinID4",
                "CPPstartDate": "14/03/2022",  # fail. fails in section47, pass in cin not considered.
            },
            # child 5: date present in section47 and absent in cin
            {
                "LAchildID": "child5",
                "CINdetailsID": "cinID4",
                "CPPstartDate": "19/07/2021",  # passes in section47
            },
            # child 6: no DateOfInitialCPC recorded in cin or section47 table
            {
                "LAchildID": "child6",
                "CINdetailsID": "cinID4",
                "CPPstartDate": "19/07/2021",  # fail
            },
            # child 8: multiple section47s in the same cin module where some pass and others fail.
            {
                "LAchildID": "child8",
                "CINdetailsID": "cinID1",
                "CPPstartDate": "20/10/2021",  # passes in section_47
            },
            # child 9: present in cin, absent in section47
            {
                "LAchildID": "child9",
                "CINdetailsID": "cinID1",
                "CPPstartDate": "20/10/2021",  # passes in cin
            },
        ]
    )
    sample_section47 = pd.DataFrame(
        [
            {  # 0 pass
                "LAchildID": "child1",
                "DateOfInitialCPC": "26/05/2021",  # pass. same as cppstartdate
                "CINdetailsID": "cinID1",
            },
            {  # 1 ignored
                "LAchildID": "child1",
                "DateOfInitialCPC": "26/05/2021",  # ignore. cppstartdate not in period of census
                "CINdetailsID": "cinID2",
            },
            {  # 2 pass
                "LAchildID": "child2",
                "DateOfInitialCPC": "30/05/2021",  # fail. not the same
                "CINdetailsID": "cinID1",
            },
            {  # 4 absent, ignored
                "LAchildID": "child3",
                "DateOfInitialCPC": "26/05/2021",  # ignore. cppstartdate is absent.
                "CINdetailsID": "cinID2",
            },
            {  # 5 fail
                "LAchildID": "child3",
                "DateOfInitialCPC": "26/05/2021",  # fail. not the same
                "CINdetailsID": "cinID3",
            },
            {  # 6 pass
                "LAchildID": "child3",
                "DateOfInitialCPC": pd.NA,  # fail. not the same
                "CINdetailsID": "cinID4",
            },
            {
                "LAchildID": "child5",
                "DateOfInitialCPC": "19/07/2021",  # pass. same as cppstartdate
                "CINdetailsID": "cinID4",
            },
            {
                "LAchildID": "child5",
                "DateOfInitialCPC": pd.NA,  # pass since other section47 in same modeule passes.
                "CINdetailsID": "cinID4",
            },
            {
                "LAchildID": "child6",
                "DateOfInitialCPC": pd.NA,  # fail. not the same
                "CINdetailsID": "cinID4",
            },
            {
                "LAchildID": "child8",
                "DateOfInitialCPC": "20/10/2021",  # pass. same as cpp_start_date
                "CINdetailsID": "cinID1",
            },
            {
                "LAchildID": "child8",
                "DateOfInitialCPC": "22/07/2021",  # pass since other section47 in the same CINmodule passes.
                "CINdetailsID": "cinID1",
            },
        ]
    )
    sample_cin_details = pd.DataFrame(
        [
            {  # 0 pass
                "LAchildID": "child1",
                "DateOfInitialCPC": "26/10/2020",  # ignore fail. not the same but present in section47 table
                "CINdetailsID": "cinID1",
            },
            {  # 1 ignore
                "LAchildID": "child1",
                "DateOfInitialCPC": "26/05/2021",  # ignore. cppstartdate not in period of census
                "CINdetailsID": "cinID2",
            },
            {  # 2 pass
                "LAchildID": "child2",
                "DateOfInitialCPC": "26/05/2021",  # ignore fail. could've passed but present in section47
                "CINdetailsID": "cinID1",
            },
            {  # 3 fail
                "LAchildID": "child3",
                "DateOfInitialCPC": "28/05/2021",  # fail. not the same and no corresponding section47
                "CINdetailsID": "cinID1",
            },
            {  # 4 ignore
                "LAchildID": "child3",
                "DateOfInitialCPC": "26/05/2021",  # ignore. cppstartdate is absent
                "CINdetailsID": "cinID2",
            },
            {  # 5 fail
                "LAchildID": "child3",
                "DateOfInitialCPC": "26/05/2003",  # ignore fail. not the same and has corresponding section47.
                "CINdetailsID": "cinID3",
            },
            {  # 6 pass
                "LAchildID": "child3",
                "DateOfInitialCPC": "14/03/2022",  # ignore fail. could've passed but present in section47
                "CINdetailsID": "cinID4",
            },
            {
                "LAchildID": "child5",
                "DateOfInitialCPC": pd.NA,  # ignore fail. not the same  and has corresponding section47.
                "CINdetailsID": "cinID4",
            },
            {
                "LAchildID": "child6",
                "DateOfInitialCPC": pd.NA,  # ignore fail. not the same and has corresponding section47
                "CINdetailsID": "cinID4",
            },
            {
                "LAchildID": "child7",
                "DateOfInitialCPC": pd.NA,  # ignore. not present in cpp table.
                "CINdetailsID": "cinID1",
            },
            {
                "LAchildID": "child8",
                "DateOfInitialCPC": pd.NA,  # passes in section47
                "CINdetailsID": "cinID1",
            },
            {
                "LAchildID": "child9",
                "CINdetailsID": "cinID1",
                "DateOfInitialCPC": "20/10/2021",  # passes in cin
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
    sample_section47[DateOfInitialCPC] = pd.to_datetime(
        sample_section47[DateOfInitialCPC], format="%d/%m/%Y", errors="coerce"
    )
    sample_cin_details[DateOfInitialCPC] = pd.to_datetime(
        sample_cin_details[DateOfInitialCPC], format="%d/%m/%Y", errors="coerce"
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
                    "child2",  # ChildID
                    "cinID1",  # CINdetailsID,
                ),
                "ROW_ID": [2],
            },
            {
                "ERROR_ID": (
                    "child3",
                    "cinID3",
                ),
                "ROW_ID": [4],
            },
            {
                "ERROR_ID": (
                    "child3",
                    "cinID4",
                ),
                "ROW_ID": [5],
            },
            {
                "ERROR_ID": (
                    "child6",
                    "cinID4",
                ),
                "ROW_ID": [8],
            },
        ]
    )
    assert issue_rows.equals(expected_df)

    # check table 2
    issues2 = issues_list[2]
    issue_table2 = issues2.table
    assert issue_table2 == CINdetails

    issue_columns2 = issues2.columns
    assert issue_columns2 == [DateOfInitialCPC]

    issue_rows2 = issues2.row_df
    assert len(issue_rows2) == 1
    assert isinstance(issue_rows2, pd.DataFrame)
    assert issue_rows2.columns.to_list() == ["ERROR_ID", "ROW_ID"]

    expected_df2 = pd.DataFrame(
        [
            {
                "ERROR_ID": (
                    "child3",  # ChildID
                    "cinID1",  # CINdetailsID,
                ),
                "ROW_ID": [3],
            },
        ]
    )
    assert issue_rows2.equals(expected_df2)

    # check table 0
    issues0 = issues_list[0]
    issue_table0 = issues0.table
    assert issue_table0 == ChildProtectionPlans

    issue_columns0 = issues0.columns
    assert issue_columns0 == [CPPstartDate]

    issue_rows0 = issues0.row_df
    assert len(issue_rows0) == 6
    assert isinstance(issue_rows0, pd.DataFrame)
    assert issue_rows0.columns.to_list() == ["ERROR_ID", "ROW_ID"]

    expected_df0 = pd.DataFrame(
        [
            {
                "ERROR_ID": (
                    "child1",  # ChildID
                    "cinID1",  # CINdetailsID,
                    pd.to_datetime("26/06/2021", format="%d/%m/%Y", errors="coerce"),
                ),
                "ROW_ID": [1],
            },
            {
                "ERROR_ID": (
                    "child2",  # ChildID
                    "cinID1",  # CINdetailsID,
                    pd.to_datetime("26/05/2021", format="%d/%m/%Y", errors="coerce"),
                ),
                "ROW_ID": [3],
            },
            {
                "ERROR_ID": (
                    "child3",  # ChildID
                    "cinID1",  # CINdetailsID,
                    pd.to_datetime("26/05/2021", format="%d/%m/%Y", errors="coerce"),
                ),
                "ROW_ID": [4],
            },
            {
                "ERROR_ID": (
                    "child3",  # ChildID
                    "cinID3",  # CINdetailsID,
                    pd.to_datetime("07/02/2022", format="%d/%m/%Y", errors="coerce"),
                ),
                "ROW_ID": [6],
            },
            {
                "ERROR_ID": (
                    "child3",  # ChildID
                    "cinID4",  # CINdetailsID,
                    pd.to_datetime("14/03/2022", format="%d/%m/%Y", errors="coerce"),
                ),
                "ROW_ID": [7],
            },
            {
                "ERROR_ID": (
                    "child6",  # ChildID
                    "cinID4",  # CINdetailsID,
                    pd.to_datetime("19/07/2021", format="%d/%m/%Y", errors="coerce"),
                ),
                "ROW_ID": [9],
            },
        ]
    )
    assert issue_rows0.equals(expected_df0)

    # Confirm that the rule details were properly pushed through.
    assert result.definition.code == "2885"
    assert (
        result.definition.message
        == "Child protection plan shown as starting a different day to the initial child protection conference."
    )
