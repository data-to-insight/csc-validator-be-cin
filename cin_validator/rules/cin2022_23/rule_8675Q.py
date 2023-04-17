from typing import Mapping

import pandas as pd

from cin_validator.rule_engine import CINTable, RuleContext, RuleType, rule_definition
from cin_validator.test_engine import run_rule
from cin_validator.utils import england_working_days, make_census_period

# Get tables and columns of interest from the CINTable object defined in rule_engine/__api.py

Section47 = CINTable.Section47
DateOfInitialCPC = Section47.DateOfInitialCPC
ICPCnotReqiured = Section47.ICPCnotRequired
S47ActualStartDate = Section47.S47ActualStartDate
LAchildID = Section47.LAchildID

Header = CINTable.Header
ReferenceDate = Header.ReferenceDate


# define characteristics of rule
@rule_definition(
    # write the rule code here, in place of 8675Q
    code="8675Q",
    rule_type=RuleType.QUERY,
    module=CINTable.Section47,
    message="Please check and either amend data or provide a reason: S47 Enquiry started more than 15 working days before the end of the census year. However, there is no date of Initial Child Protection Conference.",
    affected_fields=[DateOfInitialCPC, S47ActualStartDate, ICPCnotReqiured],
)
def validate(
    data_container: Mapping[CINTable, pd.DataFrame], rule_context: RuleContext
):
    # PREPARING DATA
    df = data_container[Section47]
    header = data_container[Header]
    # Before you begin, rename the index so that the initial row positions can be kept intact.
    df.index.name = "ROW_ID"

    ref_date_series = header[ReferenceDate]
    collection_start, collection_end = make_census_period(ref_date_series)

    # lOGIC
    # Implement rule logic as described by the Github issue.
    # Put the description as a comment above the implementation as shown.

    # If <DateOfInitialCPC> (N00110) not present and <ICPCnotReqiured> (N00111) equals false
    # then <S47ActualStartDate> (N00148) should not be before the <ReferenceDate> (N00603) minus 15 working days
    no_cpc = df[DateOfInitialCPC].isna()
    icpc_false = df[ICPCnotReqiured].astype(str).isin(["false", "0"])
    before_15b = df[S47ActualStartDate] < (collection_end - england_working_days(15))
    condition = (no_cpc & icpc_false) & (before_15b)

    # get all the data that fits the failing condition. Reset the index so that ROW_ID now becomes a column of df
    df_issues = df[condition].reset_index()

    # SUBMIT ERRORS
    # Generate a unique ID for each instance of an error.
    # Replace S47ActualStartDate and ICPCnotReqiured below with the columns concerned in your rule.
    link_id = tuple(
        zip(
            df_issues[LAchildID],
            df_issues[S47ActualStartDate],
            df_issues[ICPCnotReqiured],
        )
    )
    df_issues["ERROR_ID"] = link_id
    df_issues = (
        df_issues.groupby("ERROR_ID", group_keys=False)["ROW_ID"]
        .apply(list)
        .reset_index()
    )
    # Ensure that you do not change the ROW_ID, and ERROR_ID column names which are shown above. They are keywords in this project.
    rule_context.push_type_1(
        table=Section47,
        columns=[DateOfInitialCPC, S47ActualStartDate, ICPCnotReqiured],
        row_df=df_issues,
    )


def test_validate():
    # Create some sample data such that some values pass the validation and some fail.
    fake_header = pd.DataFrame(
        [{ReferenceDate: "31/03/2022"}]  # the census start date here will be 01/04/2021
    )

    section47 = pd.DataFrame(
        [
            {  # 0 fail, no ICPCnotrequied as true or InitialCPC, and date is more than 15 days before end of census year
                "LAchildID": "child1",
                "DateOfInitialCPC": pd.NA,
                "S47ActualStartDate": "29/01/2022",
                "ICPCnotRequired": "false",
            },
            {  # 1 ignore DateOfInitialCPC notna
                "LAchildID": "child2",
                "DateOfInitialCPC": "26/05/2000",
                "S47ActualStartDate": "26/05/2001",
                "ICPCnotRequired": "false",
            },
            {  # 2 pass. more than 15 working days before ref date
                "LAchildID": "child3",
                "DateOfInitialCPC": pd.NA,
                "S47ActualStartDate": "25/03/2022",
                "ICPCnotRequired": "false",
            },
            {  # ignore S47ActualStartDate isna
                "LAchildID": "child3",
                "DateOfInitialCPC": "26/05/2000",
                "S47ActualStartDate": pd.NA,
                "ICPCnotRequired": "false",
            },
            {  # ignore
                "LAchildID": "child5",
                "DateOfInitialCPC": pd.NA,
                "S47ActualStartDate": pd.NA,
                "ICPCnotRequired": "true",
            },
            {  # 5 fail, no ICPCnotrequied as true or InitialCPC, and date is more than 15 days before end of census year
                "LAchildID": "child6",
                "DateOfInitialCPC": pd.NA,
                "S47ActualStartDate": "29/01/2022",
                "ICPCnotRequired": "0",
            },
        ]
    )
    # if rule requires columns containing date values, convert those columns to datetime objects first. Do it here in the test_validate function, not above.
    section47["DateOfInitialCPC"] = pd.to_datetime(
        section47["DateOfInitialCPC"], format="%d/%m/%Y", errors="coerce"
    )
    section47["S47ActualStartDate"] = pd.to_datetime(
        section47["S47ActualStartDate"], format="%d/%m/%Y", errors="coerce"
    )

    # Run rule function passing in our sample data
    result = run_rule(validate, {Section47: section47, Header: fake_header})

    # Use .type1_issues to check for the result of .push_type1_issues() which you used above.
    issues = result.type1_issues

    # get table name and check it. Replace ChildProtectionPlans with the name of your table.
    issue_table = issues.table
    assert issue_table == Section47

    # check that the right columns were returned. Replace CPPstartDate and CPPendDate with a list of your columns.
    issue_columns = issues.columns
    assert issue_columns == [DateOfInitialCPC, S47ActualStartDate, ICPCnotReqiured]

    # check that the location linking dataframe was formed properly.
    issue_rows = issues.row_df
    # replace 1 with the number of failing points you expect from the sample data.
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
                    pd.to_datetime("29/01/2022", format="%d/%m/%Y", errors="coerce"),
                    "false",
                ),
                "ROW_ID": [0],
            },
            {
                "ERROR_ID": (
                    "child6",
                    pd.to_datetime("29/01/2022", format="%d/%m/%Y", errors="coerce"),
                    "0",
                ),
                "ROW_ID": [5],
            },
        ]
    )
    assert issue_rows.equals(expected_df)

    # Check that the rule definition is what you wrote in the context above.

    # replace 8675Q with the rule code and put the appropriate message in its place too.
    assert result.definition.code == "8675Q"
    assert (
        result.definition.message
        == "Please check and either amend data or provide a reason: S47 Enquiry started more than 15 working days before the end of the census year. However, there is no date of Initial Child Protection Conference."
    )
