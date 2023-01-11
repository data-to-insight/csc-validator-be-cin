"""
Rule number: 8555
Module: CIN details
Rule details: If <PersonDeathDate> (N00108) is present, then the <CINreferralDate> (N00100) must be on or before the <PersonDeathDate> (N00108)
Rule message: Child cannot be referred after its recorded date of death

"""

from typing import Mapping

import pandas as pd

from cin_validator.rule_engine import CINTable, RuleContext, rule_definition
from cin_validator.test_engine import run_rule
from cin_validator.utils import make_census_period

# Get tables and columns of interest from the CINTable object defined in rule_engine/__api.py

ChildIdentifiers = CINTable.ChildIdentifiers
PersonDeathDate = ChildIdentifiers.PersonDeathDate
LAchildID = ChildIdentifiers.LAchildID

CINdetails = CINTable.CINdetails
CINreferralDate = CINdetails.CINreferralDate
LAchildID = CINdetails.LAchildID

# define characteristics of rule
@rule_definition(
    # write the rule code here
    code=8555,
    # replace CINdetails with the value in the module column of the excel sheet corresponding to this rule .
    # Note that even if multiple tables are involved, one table will be named in the module column.
    module=CINTable.CINdetails,
    # replace the message with the corresponding value for this rule, gotten from the excel sheet.
    message="Child cannot be referred after its recorded date of death",
    # The column names tend to be the words within the < > signs in the github issue description.
    affected_fields=[
        PersonDeathDate,
        CINreferralDate,
    ],
)
def validate(
    data_container: Mapping[CINTable, pd.DataFrame], rule_context: RuleContext
):

    df_CINDetails = data_container[CINdetails].copy()
    df_ChildIdentifiers = data_container[ChildIdentifiers].copy()

    df_CINDetails.index.name = "ROW_ID"
    df_ChildIdentifiers.index.name = "ROW_ID"

    df_CINDetails.reset_index(inplace=True)
    df_ChildIdentifiers.reset_index(inplace=True)

    # Remove rows with no death date
    df_ChildIdentifiers = df_ChildIdentifiers[
        df_ChildIdentifiers[PersonDeathDate].notna()
    ]

    # <CINreferralDate> (N00100) must be on or before the <PersonDeathDate> (N00108)

    #  Join tables
    df_merged = df_CINDetails.merge(
        df_ChildIdentifiers,
        left_on=["LAchildID"],
        right_on=["LAchildID"],
        how="left",
        suffixes=("_CINDetails", "_ChildIdentifiers"),
    )

    #  Get rows where PersonDeathDate is less than  CINreferralDate
    condition = df_merged[PersonDeathDate] < df_merged[CINreferralDate]
    df_merged = df_merged[condition].reset_index()

    # Error identifier
    df_merged["ERROR_ID"] = tuple(
        zip(
            df_merged[LAchildID], df_merged[CINreferralDate], df_merged[PersonDeathDate]
        )
    )
    df_CINDetails_issues = (
        df_CINDetails.merge(df_merged, left_on="ROW_ID", right_on="ROW_ID_CINDetails")
        .groupby("ERROR_ID", group_keys="False")["ROW_ID"]
        .apply(list)
        .reset_index()
    )
    df_ChildIdentifiers_issues = (
        df_ChildIdentifiers.merge(
            df_merged, left_on="ROW_ID", right_on="ROW_ID_ChildIdentifiers"
        )
        .groupby("ERROR_ID", group_keys="False")["ROW_ID"]
        .apply(list)
        .reset_index()
    )

    # Ensure that you maintain the ROW_ID, and ERROR_ID column names which are shown above. They are keywords in this project.
    rule_context.push_type_2(
        table=CINdetails, columns=[CINreferralDate], row_df=df_CINDetails_issues
    )
    rule_context.push_type_2(
        table=ChildIdentifiers,
        columns=[PersonDeathDate],
        row_df=df_ChildIdentifiers_issues,
    )


def test_validate():
    # Create dummy data
    sample_CINDetails = pd.DataFrame(
        [
            {
                "LAchildID": "child1",
                "CINreferralDate": "26/05/2000",  # Pass as dates are the same
            },
            {
                "LAchildID": "child2",
                "CINreferralDate": "27/06/2002",  # Fails, referral after death
            },
            {
                "LAchildID": "child3",
                "CINreferralDate": "07/02/1999",  # Pass, pre death
            },
            {
                "LAchildID": "child4",
                "CINreferralDate": pd.NA,  # Ignored, no referral date
            },
            {
                "LAchildID": "child5",
                "CINreferralDate": "14/03/2001",  # Pass, dropped due to no death date
            },
        ]
    )
    sample_ChildIdentifiers = pd.DataFrame(
        [
            {
                "LAchildID": "child1",  # Pass
                "PersonDeathDate": "26/05/2000",
            },
            {
                "LAchildID": "child2",  # Fails
                "PersonDeathDate": "26/05/2000",
            },
            {
                "LAchildID": "child3",  # Pass
                "PersonDeathDate": "26/05/2000",
            },
            {
                "LAchildID": "child4",  # Pass
                "PersonDeathDate": "26/05/2000",
            },
            {
                "LAchildID": "child5",  # Pass
                "PersonDeathDate": pd.NA,
            },
        ]
    )

    # Convert date columns to dates
    sample_CINDetails[CINreferralDate] = pd.to_datetime(
        sample_CINDetails[CINreferralDate], format="%d/%m/%Y", errors="coerce"
    )
    sample_ChildIdentifiers["PersonDeathDate"] = pd.to_datetime(
        sample_ChildIdentifiers["PersonDeathDate"], format="%d/%m/%Y", errors="coerce"
    )

    # Run the rule function, passing in our sample data.
    result = run_rule(
        validate,
        {
            CINdetails: sample_CINDetails,
            ChildIdentifiers: sample_ChildIdentifiers,
        },
    )

    # Use .type2_issues to check for the result of .push_type2_issues() which you used above.
    issues_list = result.type2_issues
    assert len(issues_list) == 2
    # the function returns a list on NamedTuples where each NamedTuple contains (table, column_list, df_issues)
    # pick any table and check it's values. the tuple in location 1 will contain the ChildIdentifiers columns because that's the second thing pushed above.
    issues = issues_list[1]

    # get table name and check it. Replace ChildIdentifiers with the name of your table.
    issue_table = issues.table
    assert issue_table == ChildIdentifiers

    # check that the right columns were returned. Replace PersonDeathDate  with a list of your columns.
    issue_columns = issues.columns
    assert issue_columns == [PersonDeathDate]

    # check that the location linking dataframe was formed properly.
    issue_rows = issues.row_df
    # replace 3 with the number of failing points you expect from the sample data.
    assert len(issue_rows) == 1
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
                    # Start Date
                    pd.to_datetime("27/06/2002", format="%d/%m/%Y", errors="coerce"),
                    # Review date
                    pd.to_datetime("26/05/2000", format="%d/%m/%Y", errors="coerce"),
                ),
                "ROW_ID": [1],
            },
        ]
    )
    assert issue_rows.equals(expected_df)

    # Check that the rule definition is what you wrote in the context above.

    # replace 2885 with the rule code and put the appropriate message in its place too.
    assert result.definition.code == 8555
    assert (
        result.definition.message
        == "Child cannot be referred after its recorded date of death"
    )
