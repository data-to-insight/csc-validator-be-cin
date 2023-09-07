from typing import Mapping

import pandas as pd

from cin_validator.rule_engine import CINTable, RuleContext, rule_definition
from cin_validator.test_engine import run_rule

# Get tables and columns of interest from the CINTable object defined in rule_engine/__api.py

CINdetails = CINTable.CINdetails
Section47 = CINTable.Section47

CINclosureDate = CINdetails.CINclosureDate
LAchildID = CINdetails.LAchildID
CINdetailsID = CINdetails.CINdetailsID
DateOfInitialCPC = Section47.DateOfInitialCPC
ICPCnotRequired = Section47.ICPCnotRequired


# define characteristics of rule
@rule_definition(
    code="8868",
    # replace CINdetails with the value in the module column of the excel sheet corresponding to this rule .
    # Note that even if multiple tables are involved, one table will be named in the module column.
    module=CINTable.CINdetails,
    # replace the message with the corresponding value for this rule, gotten from the excel sheet.
    message="CIN episode is shown as closed, however Section 47 enquiry is not shown as completed by ICPC date or ICPC not required flag",
    # The column names tend to be the words within the < > signs in the github issue description.
    affected_fields=[
        ICPCnotRequired,
        CINclosureDate,
        DateOfInitialCPC,
    ],
)
def validate(
    data_container: Mapping[CINTable, pd.DataFrame], rule_context: RuleContext
):
    # PREPARING DATA

    df_47 = data_container[Section47].copy()
    df_cin = data_container[CINdetails].copy()

    # Before you begin, rename the index so that the initial row positions can be kept intact.
    df_47.index.name = "ROW_ID"
    df_cin.index.name = "ROW_ID"

    df_47.reset_index(inplace=True)
    df_cin.reset_index(inplace=True)

    # If <CINclosureDate> (N00102) is present then all instances of the <Section47> group must either
    # i) include the <DateOfInitialCPC> (N00110) or
    # ii) include <ICPCnotRequired> (N00111) with a value of true or 1

    CINclosure_present = df_cin[df_cin[CINclosureDate].notna()]

    # Children with no s47 module must be excluded to prevent falsely flagging them, done usig inner merge.
    merged_df = CINclosure_present.merge(
        df_47,
        on=[
            LAchildID,
            "CINdetailsID",
        ],
        how="inner",
        suffixes=["_cin", "_47"],
    )

    # Checks DateOfInitialCPC from s47 model
    condition_1 = (merged_df["DateOfInitialCPC_47"].isna()) & (
        ~merged_df[ICPCnotRequired].isin(["1", "true"])
    )
    condition_2 = (merged_df["DateOfInitialCPC_47"].notna()) & (
        merged_df[ICPCnotRequired].isin(["1", "true"])
    )

    # get all the data that fits the failing condition.
    merged_df = merged_df[condition_1 | condition_2].reset_index()

    # create an identifier for each error instance.
    merged_df["ERROR_ID"] = tuple(zip(merged_df[LAchildID], merged_df[CINdetailsID]))

    # we can now map the suffixes columns to their corresponding source tables such that the failing ROW_IDs and ERROR_IDs exist per table.
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

    # Ensure that you maintain the ROW_ID, and ERROR_ID column names which are shown above. They are keywords in this project.

    rule_context.push_type_2(
        table=Section47,
        columns=[DateOfInitialCPC, ICPCnotRequired],
        row_df=df_47_issues,
    )
    rule_context.push_type_2(
        table=CINdetails, columns=[CINclosureDate], row_df=df_cin_issues
    )


def test_validate():
    # Create some sample data such that some values pass the validation and some fail.
    sample_section47 = pd.DataFrame(
        [
            {
                "LAchildID": "child1",
                "DateOfInitialCPC": pd.NA,  # 0 fail: DateOfInitialCPC missing and ICPCnotRequired false
                "ICPCnotRequired": "false",
                "CINdetailsID": "cinID1",
            },
            {
                "LAchildID": "child1",
                "DateOfInitialCPC": pd.NA,  # 1 pass ICPCnotRequired true
                "ICPCnotRequired": "true",
                "CINdetailsID": "cinID2",
            },
            {
                "LAchildID": "child2",
                "DateOfInitialCPC": pd.NA,
                "ICPCnotRequired": "1",  # 2 pass ICPCnotRequired true
                "CINdetailsID": "cinID1",
            },
            {
                "LAchildID": "child3",
                "DateOfInitialCPC": "27/05/2000",  # 3 pass DateOfInitialCPC present
                "ICPCnotRequired": "false",
                "CINdetailsID": "cinID1",
            },
            {
                "LAchildID": "child3",
                "DateOfInitialCPC": "26/10/1999",  # 4 fail
                "ICPCnotRequired": "nottrue",
                "CINdetailsID": "cinID2",
            },
            {
                "LAchildID": "child3",
                "DateOfInitialCPC": "26/05/2000",  # 5 pass DateOfInitialCPC present
                "ICPCnotRequired": "1",
                "CINdetailsID": "cinID2",
            },
            {
                "LAchildID": "child4",
                "DateOfInitialCPC": pd.NA,
                "ICPCnotRequired": "false",  # 6 ignore
                "CINdetailsID": "cinID4",
            },
        ]
    )
    sample_cin = pd.DataFrame(
        [
            {
                "LAchildID": "child1",  # 0 fail: contains a section47 group that fails.
                "CINclosureDate": "26/10/1999",
                "CINdetailsID": "cinID1",
                "DateOfInitialCPC": "26/10/2001",
            },
            {
                "LAchildID": "child1",  # 1 pass
                "CINclosureDate": "26/05/2000",
                "CINdetailsID": "cinID2",
                "DateOfInitialCPC": pd.NA,
            },
            {
                "LAchildID": "child2",  # 2
                "CINclosureDate": "26/05/2000",
                "CINdetailsID": "cinID1",
                "DateOfInitialCPC": pd.NA,
            },
            {
                "LAchildID": "child3",  # 3 pass
                "CINclosureDate": "28/05/2000",
                "CINdetailsID": "cinID1",
                "DateOfInitialCPC": pd.NA,
            },
            {
                "LAchildID": "child3",  # 4 fail: contains a section47 group that fails.
                "CINclosureDate": "26/05/2000",
                "CINdetailsID": "cinID2",
                "DateOfInitialCPC": pd.NA,
            },
            {
                "LAchildID": "child3",  # 5 Pass, not present in section47 table so none of the values meets the requirements
                "CINclosureDate": "26/05/2003",
                "CINdetailsID": "cinID3",
                "DateOfInitialCPC": pd.NA,
            },
            {
                "LAchildID": "child4",
                "CINclosureDate": pd.NA,  # 6 ignore: date absent
                "CINdetailsID": "cinID4",
                "DateOfInitialCPC": pd.NA,
            },
        ]
    )
    # if rule requires columns containing date values, convert those columns to datetime objects first. Do it here in the test_validate function, not above.
    sample_cin[CINclosureDate] = pd.to_datetime(
        sample_cin[CINclosureDate], format="%d/%m/%Y", errors="coerce"
    )
    sample_section47["DateOfInitialCPC"] = pd.to_datetime(
        sample_section47["DateOfInitialCPC"], format="%d/%m/%Y", errors="coerce"
    )
    sample_cin["DateOfInitialCPC"] = pd.to_datetime(
        sample_cin["DateOfInitialCPC"], format="%d/%m/%Y", errors="coerce"
    )

    # Run the rule function, passing in our sample data.
    result = run_rule(
        validate,
        {
            Section47: sample_section47,
            CINdetails: sample_cin,
        },
    )

    # Use .type2_issues to check for the result of .push_type2_issues() which you used above.
    issues_list = result.type2_issues
    assert len(issues_list) == 2
    # the function returns a list on NamedTuples where each NamedTuple contains (table, column_list, df_issues)
    # pick any table and check it's values. the tuple in location 1 will contain the CINdetails columns because that's the second thing pushed above.
    issues = issues_list[1]

    # get table name and check it. Replace CINdetails with the name of your table.
    issue_table = issues.table
    assert issue_table == CINdetails

    # check that the right columns were returned. Replace CINclosureDate with a list of your columns.
    issue_columns = issues.columns
    assert issue_columns == [CINclosureDate]

    # check that the location linking dataframe was formed properly.
    issue_rows = issues.row_df
    # replace 3 with the number of failing points you expect from the sample data.
    assert len(issue_rows) == 2
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
                    "child3",  # ChildID
                    "cinID2",  # CINdetailsID
                ),
                "ROW_ID": [4],
            },
        ]
    )
    assert issue_rows.equals(expected_df)

    # Check that the rule definition is what you wrote in the context above.

    # replace '8868' with the rule code and put the appropriate message in its place too.
    assert result.definition.code == "8868"
    assert (
        result.definition.message
        == "CIN episode is shown as closed, however Section 47 enquiry is not shown as completed by ICPC date or ICPC not required flag"
    )
