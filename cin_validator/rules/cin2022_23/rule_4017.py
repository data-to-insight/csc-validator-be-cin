from typing import Mapping

import pandas as pd

from cin_validator.rule_engine import CINTable, RuleContext, rule_definition
from cin_validator.test_engine import run_rule
from cin_validator.utils import make_census_period

# Get tables and columns of interest from the CINTable object defined in rule_engine/__api.py

ChildProtectionPlans = CINTable.ChildProtectionPlans
LAchildID = ChildProtectionPlans.LAchildID
CPPID = ChildProtectionPlans.CPPID
CPPstartDate = ChildProtectionPlans.CPPstartDate
CPPendDate = ChildProtectionPlans.CPPendDate

CINplanDates = CINTable.CINplanDates
CINPlanStartDate = CINplanDates.CINPlanStartDate
CINPlanEndDate = CINplanDates.CINPlanEndDate

Header = CINTable.Header
ReferenceDate = Header.ReferenceDate


# define characteristics of rule
@rule_definition(
    # write the rule code here
    code=4017,
    # replace ChildProtectionPlans with the value in the module column of the excel sheet corresponding to this rule .
    # Note that even if multiple tables are involved, one table will be named in the module column.
    module=CINTable.ChildProtectionPlans,
    # replace the message with the corresponding value for this rule, gotten from the excel sheet.
    message="A CIN Plan has been reported as open at the same time as a Child Protection Plan.",
    # The column names tend to be the words within the < > signs in the github issue description.
    affected_fields=[
        CINPlanStartDate,
        CINPlanEndDate,
        CPPstartDate,
    ],
)
def validate(
    data_container: Mapping[CINTable, pd.DataFrame], rule_context: RuleContext
):
    # PREPARING DATA

    # Replace ChildProtectionPlans with the name of the table you need.
    df_cpp = data_container[ChildProtectionPlans].copy()
    df_cin = data_container[CINplanDates].copy()

    # Before you begin, rename the index so that the initial row positions can be kept intact.
    df_cpp.index.name = "ROW_ID"
    df_cin.index.name = "ROW_ID"

    # Resetting the index causes the ROW_IDs to become columns of their respective DataFrames
    # so that they can come along when the merge is done.
    df_cpp.reset_index(inplace=True)
    df_cin.reset_index(inplace=True)

    # ReferenceDate exists in the header table so we get header table too.
    df_ref = data_container[Header]
    ref_date_series = df_ref[ReferenceDate]

    # the make_census_period function generates the start and end date so that you don't have to do it each time.
    collection_start, reference_date = make_census_period(ref_date_series)

    # lOGIC
    # Implement rule logic as described by the Github issue.
    # Put the description as a comment above the implementation as shown.

    # The <CINPlanStartDate> (N00689) for any CIN Plan group cannot fall within either:
    # <CPPstartDate> (N00105) or <CPPendDate> (N00115);
    # or <CPPstartDate> and <ReferenceDate> (N00603) if <CPPendDate> is not present - for any CPP group;
    # unless <CINPlanStartDate> is equal to <CPPendDate> for this group

    #  Merge tables
    df_merged = df_cpp.merge(
        df_cin,
        on=["LAchildID"],
        how="left",
        suffixes=("_cpp", "_cin"),
    )

    # Get rows where CPPstartDate is after CINPlanStartDate
    # and CPPstartDate before CINPlanEndDate (or if null, before/on ReferenceDate)
    cpp_start_after_cin_start = df_merged[CPPstartDate] >= df_merged[CINPlanStartDate]
    cpp_start_before_cin_end = (
        df_merged[CPPstartDate] < df_merged[CINPlanEndDate]
    ) & df_merged[CINPlanEndDate].notna()
    cpp_start_before_reference_date = (
        df_merged[CPPstartDate] <= reference_date
    ) & df_merged[CINPlanEndDate].isna()

    df_merged = df_merged[
        cpp_start_after_cin_start
        & (cpp_start_before_cin_end | cpp_start_before_reference_date)
    ].reset_index()

    # create an identifier for each error instance.
    df_merged["ERROR_ID"] = tuple(zip(df_merged[LAchildID], df_merged[CPPstartDate]))

    # The merges were done on copies of df_cpp and df_cin so that the column names in dataframes themselves aren't affected by the suffixes.
    # we can now map the suffixes columns to their corresponding source tables such that the failing ROW_IDs and ERROR_IDs exist per table.
    df_cpp_issues = (
        df_cpp.merge(df_merged, left_on="ROW_ID", right_on="ROW_ID_cpp")
        .groupby("ERROR_ID", group_keys=False)["ROW_ID"]
        .apply(list)
        .reset_index()
    )
    df_cin_issues = (
        df_cin.merge(df_merged, left_on="ROW_ID", right_on="ROW_ID_cin")
        .groupby("ERROR_ID", group_keys=False)["ROW_ID"]
        .apply(list)
        .reset_index()
    )

    # Ensure that you maintain the ROW_ID, and ERROR_ID column names which are shown above. They are keywords in this project.
    rule_context.push_type_2(
        table=ChildProtectionPlans,
        columns=[CPPstartDate],
        row_df=df_cpp_issues,
    )
    rule_context.push_type_2(
        table=CINplanDates,
        columns=[CINPlanStartDate, CINPlanEndDate],
        row_df=df_cin_issues,
    )


def test_validate():
    # Create some sample data such that some values pass the validation and some fail.
    sample_header = pd.DataFrame(
        [{ReferenceDate: "31/03/2001"}]  # the census start date here will be 01/04/2000
    )

    sample_cin = pd.DataFrame(
        [
            {
                "LAchildID": "child1",
                "CINPlanStartDate": "26/05/2000",
                "CINPlanEndDate": "30/05/2000",
            },
            {
                "LAchildID": "child1",
                "CINPlanStartDate": "26/06/2000",
                "CINPlanEndDate": pd.NA,
            },
            {
                "LAchildID": "child2",
                "CINPlanStartDate": "26/10/2000",
                "CINPlanEndDate": "10/12/2000",
            },
            {
                "LAchildID": "child2",
                "CINPlanStartDate": "26/02/2001",
                "CINPlanEndDate": pd.NA,
            },
            {
                "LAchildID": "child3",
                "CINPlanStartDate": "26/05/2000",
                "CINPlanEndDate": "30/10/2001",
            },
            {
                "LAchildID": "child5",
                "CINPlanStartDate": "26/05/2000",
                "CINPlanEndDate": pd.NA,
            },
        ]
    )
    sample_cpp = pd.DataFrame(
        [
            {
                "LAchildID": "child1",  # 0 Pass - Before CIN
                "CPPstartDate": "04/04/2000",
            },
            {
                "LAchildID": "child1",  # 1 Fail - During CIN
                "CPPstartDate": "28/05/2000",
            },
            {
                "LAchildID": "child1",  # 2 Pass - Same as CIN End
                "CPPstartDate": "30/05/2000",
            },
            {
                "LAchildID": "child1",  # 3 Pass - Between CIN
                "CPPstartDate": "04/06/2000",
            },
            {
                "LAchildID": "child1",  # 4 Fail - During CIN (via reference_date)
                "CPPstartDate": "30/06/2000",
            },
            {
                "LAchildID": "child2",  # 5 Fail - Same as CIN Start
                "CPPstartDate": "26/10/2000",
            },
            {
                "LAchildID": "child2",  # 6 Fail - Same as CIN Start
                "CPPstartDate": "26/02/2001",
            },
            {
                "LAchildID": "child2",  # 7 Fail - During CIN (via reference_date)
                "CPPstartDate": "26/03/2001",
            },
            {
                "LAchildID": "child3",  # 8 Pass - Same as CIN End (future return year handled by different rule!)
                "CPPstartDate": "30/10/2001",
            },
            {
                "LAchildID": "child4",  # 9 Pass - No CIN
                "CPPstartDate": "04/06/2000",
            },
            {
                "LAchildID": "child5",  # 10 Fail - Start on ReferenceDate
                "CPPstartDate": "31/03/2001",
            },
        ]
    )

    # If rule requires columns containing date values, convert those columns to datetime objects first. Do it here in the test_validate function, not above.
    sample_cpp[CPPstartDate] = pd.to_datetime(
        sample_cpp[CPPstartDate], format="%d/%m/%Y", errors="coerce"
    )
    sample_cin[CINPlanStartDate] = pd.to_datetime(
        sample_cin[CINPlanStartDate], format="%d/%m/%Y", errors="coerce"
    )
    sample_cin[CINPlanEndDate] = pd.to_datetime(
        sample_cin[CINPlanEndDate], format="%d/%m/%Y", errors="coerce"
    )
    sample_header[ReferenceDate] = pd.to_datetime(
        sample_header[ReferenceDate], format="%d/%m/%Y", errors="coerce"
    )

    # Run the rule function, passing in our sample data.
    result = run_rule(
        validate,
        {
            ChildProtectionPlans: sample_cpp,
            CINplanDates: sample_cin,
            Header: sample_header,
        },
    )

    # Use .type2_issues to check for the result of .push_type2_issues() which you used above.
    issues_list = result.type2_issues
    assert len(issues_list) == 2
    # the function returns a list on NamedTuples where each NamedTuple contains (table, column_list, df_issues)
    # pick any table and check it's values. the tuple in location 1 will contain the Reviews columns because that's the second thing pushed above.
    issues = issues_list[0]

    # get table name and check it. Replace Reviews with the name of your table.
    issue_table = issues.table
    assert issue_table == ChildProtectionPlans

    # check that the right columns were returned. Replace CPPreviewDate  with a list of your columns.
    issue_columns = issues.columns
    assert issue_columns == [CPPstartDate]

    # check that the location linking dataframe was formed properly.
    issue_rows = issues.row_df
    # replace 3 with the number of failing points you expect from the sample data.
    assert len(issue_rows) == 6
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
                    "child1",
                    pd.to_datetime("28/05/2000", format="%d/%m/%Y", errors="coerce"),
                ),
                "ROW_ID": [1],
            },
            {
                "ERROR_ID": (
                    "child1",
                    pd.to_datetime("30/06/2000", format="%d/%m/%Y", errors="coerce"),
                ),
                "ROW_ID": [4],
            },
            {
                "ERROR_ID": (
                    "child2",
                    pd.to_datetime("26/10/2000", format="%d/%m/%Y", errors="coerce"),
                ),
                "ROW_ID": [5],
            },
            {
                "ERROR_ID": (
                    "child2",
                    pd.to_datetime("26/02/2001", format="%d/%m/%Y", errors="coerce"),
                ),
                "ROW_ID": [6],
            },
            {
                "ERROR_ID": (
                    "child2",
                    pd.to_datetime("26/03/2001", format="%d/%m/%Y", errors="coerce"),
                ),
                "ROW_ID": [7],
            },
            {
                "ERROR_ID": (
                    "child5",
                    pd.to_datetime("31/03/2001", format="%d/%m/%Y", errors="coerce"),
                ),
                "ROW_ID": [10],
            },
        ]
    )
    assert issue_rows.equals(expected_df)

    # Check that the rule definition is what you wrote in the context above.

    # replace 2885 with the rule code and put the appropriate message in its place too.
    assert result.definition.code == 4017
    assert (
        result.definition.message
        == "A CIN Plan has been reported as open at the same time as a Child Protection Plan."
    )
