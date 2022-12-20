from typing import Mapping

import pandas as pd

from cin_validator.rule_engine import CINTable, RuleContext, rule_definition
from cin_validator.test_engine import run_rule
from cin_validator.utils import make_census_period

# Get tables and columns of interest from the CINTable object defined in rule_engine/__api.py

ChildIdentifiers = CINTable.ChildIdentifiers
CINdetails = CINTable.CINdetails
Disabilities = CINTable.Disabilities

LAchildID = ChildIdentifiers.LAchildID
PersonBirthDate = ChildIdentifiers.PersonBirthDate

ReferralNFA = CINdetails.ReferralNFA

Disability = Disabilities.Disability

# define characteristics of rule
@rule_definition(
    # write the rule code here, in place of 2885
    code=8540,
    # replace ChildProtectionPlans with the value in the module column of the excel sheet corresponding to this rule .
    # Note that even if multiple tables are involved, one table will be named in the module column.
    module=CINTable.Disabilities,
    # replace the message with the corresponding value for this rule, gotten from the excel sheet.
    message="Child’s disability is missing or invalid (see Disability table)",
    # The column names tend to be the words within the < > signs in the github issue description.
    affected_fields=[Disability],
)
def validate(
    data_container: Mapping[CINTable, pd.DataFrame], rule_context: RuleContext
):

    df_dis = data_container[Disabilities].copy()
    df_ci = data_container[ChildIdentifiers].copy()
    df_cin = data_container[CINdetails].copy()

    df_ci.index.name = "ROW_ID"
    df_dis.index.name = "ROW_ID"
    df_cin.index.name = "ROW_ID"

    df_ci.reset_index(inplace=True)
    df_dis.reset_index(inplace=True)
    df_cin.reset_index(inplace=True)

    # If <PersonBirthDate> (N00066) is present and at least one <ReferralNFA> (N00112) is false or 0,
    # then one or more instances of <Disability> (N00099) must be present,
    # and all instances must be valid code values
    valid_dis = [
        "NONE",
        "MOB",
        "HAND",
        "PC",
        "INC",
        "COMM",
        "LD",
        "HEAR",
        "VIS",
        "BEH",
        "CON",
        "AUT",
        "DDA",
    ]
    falseorzero = [
        "false",
        "0",
    ]

    df_ci_cin = df_ci.merge(
        df_cin, on="LAchildID", how="left", suffixes=("_ci", "_cin")
    )
    merged_df = df_ci_cin.merge(
        df_dis, on="LAchildID", how="left", suffixes=("", "_dis")
    )

    merged_df = merged_df[~merged_df[Disability].isin(valid_dis)]
    merged_df = merged_df[merged_df[PersonBirthDate].notna()]
    merged_df = merged_df[merged_df[ReferralNFA].isin(falseorzero)]

    # create an identifier for each error instance.
    # In this case, the rule is checked for each CPPstartDate, in each CINplanDates group (differentiated by CINdetailsID), in each child (differentiated by LAchildID)
    # So, a combination of LAchildID, CINdetailsID and CPPstartDate identifies and error instance.
    # You could also consider that CPPstartDate, unlike DateOfInitialCPC, is the leading column against which columns from the other tables are compared. So it is included in the zip.
    merged_df["ERROR_ID"] = tuple(
        zip(
            merged_df[LAchildID],
            merged_df[PersonBirthDate],
        )
    )
    
    df_dis_issues = (
        df_dis.merge(merged_df, on="ROW_ID")
        .groupby("ERROR_ID", group_keys=False)["ROW_ID"]
        .apply(list)
        .reset_index()
    )
    df_ci_issues = (
        df_ci.merge(merged_df, left_on="ROW_ID", right_on="ROW_ID_ci")
        .groupby("ERROR_ID", group_keys=False)["ROW_ID_x"]
        .apply(list)
        .reset_index()
    )
    df_cin_issues = (
        df_cin.merge(merged_df, left_on="ROW_ID", right_on="ROW_ID_cin")
        .groupby("ERROR_ID", group_keys=False)["ROW_ID_x"]
        .apply(list)
        .reset_index()
    )

    # Ensure that you maintain the ROW_ID, and ERROR_ID column names which are shown above. They are keywords in this project.
    rule_context.push_type_2(
        table=ChildIdentifiers, columns=[PersonBirthDate], row_df=df_ci_issues
    )
    rule_context.push_type_2(
        table=Disabilities, columns=[Disability], row_df=df_dis_issues
    )
    rule_context.push_type_2(
        table=CINdetails, columns=[ReferralNFA], row_df=df_cin_issues
    )


def test_validate():
    # Create some sample data such that some values pass the validation and some fail.
    sample_ci = pd.DataFrame(
        [
            {
                "LAchildID": "child1",
                "PersonBirthDate": "01/01/1880",
            },
            {
                "LAchildID": "child2",
                "PersonBirthDate": "01/01/1880",
            },
            {
                "LAchildID": "child3",
                "PersonBirthDate": "01/01/1880",
            },
            {
                "LAchildID": "child4",
                "PersonBirthDate": "01/01/1880",
            },
            {
                "LAchildID": "child5",
                "PersonBirthDate": "01/01/1880",
            },
        ]
    )

    sample_cin = pd.DataFrame(
        [
            {
                "LAchildID": "child1",
                "ReferralNFA": "false",
            },
            {
                "LAchildID": "child2",
                "ReferralNFA": "0",
            },
            {
                "LAchildID": "child3",
                "ReferralNFA": "0",
            },
            {
                "LAchildID": "child4",
                "ReferralNFA": "1",
            },
        ]
    )

    sample_dis = pd.DataFrame(
        [
            {
                "LAchildID": "child1",
                "Disability": pd.NA,
            },
            {
                "LAchildID": "child2",
                "Disability": "notreal",
            },
            {
                "LAchildID": "child3",
                "Disability": "MOB",
            },
        ]
    )

    # Run the rule function, passing in our sample data.
    result = run_rule(
        validate,
        {
            ChildIdentifiers: sample_ci,
            Disabilities: sample_dis,
            CINdetails: sample_cin,
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
    assert issue_table == Disabilities

    # check that the right columns were returned. Replace DateOfInitialCPC  with a list of your columns.
    issue_columns = issues.columns
    assert issue_columns == [Disability]

    # check that the location linking dataframe was formed properly.
    issue_rows = issues.row_df
    # replace 2 with the number of failing points you expect from the sample data.
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
                    "child1",
                    "01/01/1880",
                ),
                "ROW_ID": [0],
            },
            {
                "ERROR_ID": (
                    "child2",
                    "01/01/1880",
                ),
                "ROW_ID": [1],
            },
        ]
    )
    assert issue_rows.equals(expected_df)

    # Check that the rule definition is what you wrote in the context above.

    # replace 2885 with the rule code and put the appropriate message in its place too.
    assert result.definition.code == 8540
    assert (
        result.definition.message
        == "Child’s disability is missing or invalid (see Disability table)"
    )
