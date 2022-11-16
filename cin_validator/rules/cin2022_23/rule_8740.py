from typing import Mapping

import pandas as pd

from cin_validator.rule_engine import CINTable, RuleContext, rule_definition
from cin_validator.test_engine import run_rule
from cin_validator.utils import make_census_period

# Get tables and columns of interest from the CINTable object defined in rule_engine/__api.py

Section47 = CINTable.Section47
LAchildID = Section47.LAchildID
S47ActualStartDate = Section47.S47ActualStartDate
DateOfInitialCPC = Section47.DateOfInitialCPC
ICPCnotRequired = Section47.ICPCnotRequired

Header = CINTable.Header
ReferenceDate = Header.ReferenceDate

# define characteristics of rule
@rule_definition(
    # write the rule code here, in place of 8500
    code=8740,
    # replace ChildIdentifiers with the value in the module column of the excel sheet corresponding to this rule .
    module=CINTable.Section47,
    # replace the message with the corresponding value for this rule, gotten from the excel sheet.
    message="For a Section 47 Enquiry that has not held the Initial Child Protection Conference by the end of the census year, the start date must fall within the census year",
    # The column names tend to be the words within the < > signs in the github issue description.
    affected_fields=[S47ActualStartDate, DateOfInitialCPC, ICPCnotRequired],
)
def validate(
    data_container: Mapping[CINTable, pd.DataFrame], rule_context: RuleContext
):
    # PREPARING DATA

    # Replace ChildIdentifiers with the name of the table you need.
    df = data_container[Section47]
    # Before you begin, rename the index so that the initial row positions can be kept intact.
    df.index.name = "ROW_ID"

    # ReferenceDate exists in the heder table so we get header table too.
    df_ref = data_container[Header]
    ref_date_series = df_ref[ReferenceDate]

    # the make_census_period function generates the start and end date so that you don't have to do it each time.
    collection_start, reference_date = make_census_period(ref_date_series)

    # lOGIC
    # Implement rule logic as described by the Github issue.
    # Put the description as a comment above the implementation as shown.
    print("\n initial df \n", df)
    # If a <Section47> group does not contain the <DateOfInitialCPC> (N00110) and <ICPCnotRequired> (N00111) is false
    # then the <S47ActualStartDate> (N00148) must be on or between [Start_Of_Census_Year] and <ReferenceDate> (N00603)
    condition = (
        df[DateOfInitialCPC].isna()
        & (df[ICPCnotRequired].astype(str) == "0")
        & (
            (df[S47ActualStartDate] < collection_start)
            | (df[S47ActualStartDate] > reference_date)
        )
    )
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
    link_id = tuple(zip(df_issues[LAchildID], df_issues[S47ActualStartDate]))
    df_issues["ERROR_ID"] = link_id
    print("\n df_issues with link_id \n", df_issues)
    df_issues = df_issues.groupby("ERROR_ID")["ROW_ID"].apply(list).reset_index()
    # Ensure that you do not change the ROW_ID, and ERROR_ID column names which are shown above. They are keywords in this project.
    rule_context.push_type_1(
        table=Section47,
        columns=[S47ActualStartDate, DateOfInitialCPC, ICPCnotRequired],
        row_df=df_issues,
    )
    print("\n df_issues after groupby \n", df_issues)


def test_validate():
    # Create some sample data such that some values pass the validation and some fail.

    fake_header = pd.DataFrame(
        [{ReferenceDate: "31/03/2001"}]  # the census start date here will be 01/04/2000
    )

    section_47s = pd.DataFrame(
        [
            {  # 0 - Ignore
                "LAchildID": "child1",
                "S47ActualStartDate": "26/05/2000",
                "DateOfInitialCPC": "26/05/2000",
                "ICPCnotRequired": "0",
            },
            {  # 1 - Ignore
                "LAchildID": "child1",
                "S47ActualStartDate": "26/05/2000",
                "DateOfInitialCPC": "26/05/2000",
                "ICPCnotRequired": "1",
            },
            {  # 2 - Ignore
                "LAchildID": "child1",
                "S47ActualStartDate": "26/05/2000",
                "DateOfInitialCPC": pd.NA,
                "ICPCnotRequired": "1",
            },
            {  # 3 - Pass
                "LAchildID": "child1",
                "S47ActualStartDate": "26/05/2000",
                "DateOfInitialCPC": pd.NA,
                "ICPCnotRequired": "0",
            },
            {  # 4 - Fail
                "LAchildID": "child1",
                "S47ActualStartDate": "26/05/1999",
                "DateOfInitialCPC": pd.NA,
                "ICPCnotRequired": "0",
            },
            {  # 5 - Fail
                "LAchildID": "child1",
                "S47ActualStartDate": "26/05/1999",
                "DateOfInitialCPC": pd.NA,
                "ICPCnotRequired": "0",
            },
            {  # 6 - Ignore
                "LAchildID": "child1",
                "S47ActualStartDate": "26/05/1999",
                "DateOfInitialCPC": pd.NA,
                "ICPCnotRequired": "1",
            },
        ]
    )
    # if rule requires columns containing date values, convert those columns to datetime objects first. Do it here in the test_validate function, not above.
    section_47s[S47ActualStartDate] = pd.to_datetime(
        section_47s[S47ActualStartDate], format="%d/%m/%Y", errors="coerce"
    )
    section_47s[DateOfInitialCPC] = pd.to_datetime(
        section_47s[DateOfInitialCPC], format="%d/%m/%Y", errors="coerce"
    )

    # Run rule function passing in our sample data
    result = run_rule(validate, {Section47: section_47s, Header: fake_header})

    # Use .type1_issues to check for the result of .push_type1_issues() which you used above.
    issues = result.type1_issues

    # get table name and check it. Replace ChildProtectionPlans with the name of your table.
    issue_table = issues.table
    assert issue_table == Section47

    # check that the right columns were returned. Replace CPPstartDate and CPPendDate with a list of your columns.
    issue_columns = issues.columns
    assert issue_columns == [S47ActualStartDate, DateOfInitialCPC, ICPCnotRequired]

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
                    pd.to_datetime("26/05/1999", format="%d/%m/%Y", errors="coerce"),
                ),
                "ROW_ID": [4],
            },
            {
                "ERROR_ID": (
                    "child1",
                    pd.to_datetime("26/05/1999", format="%d/%m/%Y", errors="coerce"),
                ),
                "ROW_ID": [5],
            },
        ]
    )
    assert issue_rows.equals(expected_df)
    print("\n issue_rows \n", issue_rows)
    print("\n expected_df \n", expected_df)

    # Check that the rule definition is what you wrote in the context above.

    # replace 8925 with the rule code and put the appropriate message in its place too.
    assert result.definition.code == 8740
    assert (
        result.definition.message
        == "For a Section 47 Enquiry that has not held the Initial Child Protection Conference by the end of the census year, the start date must fall within the census year"
    )
