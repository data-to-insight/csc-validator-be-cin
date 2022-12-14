"""
Rule number: 8606
Module: CIN details
Rule details: <CINreferralDate> (N00100) cannot be more than 280 days before <PersonBirthDate> (N00066) or <ExpectedPersonBirthDate> (N00098)
Rule message: Child referral date is more than 40 weeks before DOB or expected DOB

"""

from typing import Mapping

import pandas as pd
import datetime as dt

from cin_validator.rule_engine import CINTable, RuleContext, rule_definition
from cin_validator.test_engine import run_rule
from cin_validator.utils import make_census_period

# Get tables and columns of interest from the CINTable object defined in rule_engine/__api.py

ChildIdentifiers = CINTable.ChildIdentifiers
PersonBirthDate = ChildIdentifiers.PersonBirthDate
ExpectedPersonBirthDate = ChildIdentifiers.ExpectedPersonBirthDate
LAchildID = ChildIdentifiers.LAchildID

CINdetails = CINTable.CINdetails
CINreferralDate = CINdetails.CINreferralDate
LAchildID = CINdetails.LAchildID

# define characteristics of rule
@rule_definition(
    code=8606,
    module=CINTable.CINdetails,
    message="Child referral date is more than 40 weeks before DOB or expected DOB",
    affected_fields=[
        CINreferralDate,
        PersonBirthDate,
        ExpectedPersonBirthDate,
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

    # <CINreferralDate> (N00100) cannot be more than 280 days before <PersonBirthDate> (N00066) or <ExpectedPersonBirthDate>

    #  Join tables
    df_merged = df_CINDetails.merge(
        df_ChildIdentifiers,
        left_on=["LAchildID"],
        right_on=["LAchildID"],
        how="left",
        suffixes=("_CINDetails", "_ChildIdentifiers"),
    )

    # # Get rows where CINreferralDate is earlier than birth/expected birth -280
    condition1 = df_merged[CINreferralDate] < (
        df_merged[PersonBirthDate] - dt.timedelta(280)
    )
    condition2 = df_merged[CINreferralDate] < (
        df_merged[ExpectedPersonBirthDate] - dt.timedelta(280)
    )
    df_merged = df_merged[condition1 | condition2].reset_index()

    # Error identifier
    df_merged["ERROR_ID"] = tuple(
        zip(
            df_merged[LAchildID],
            df_merged[CINreferralDate],
            df_merged[PersonBirthDate],
            df_merged[ExpectedPersonBirthDate],
        )
    )
    df_CINDetails_issues = (
        df_CINDetails.merge(df_merged, left_on="ROW_ID", right_on="ROW_ID_CINDetails")
        .groupby("ERROR_ID")["ROW_ID"]
        .apply(list)
        .reset_index()
    )
    df_ChildIdentifiers_issues = (
        df_ChildIdentifiers.merge(
            df_merged, left_on="ROW_ID", right_on="ROW_ID_ChildIdentifiers"
        )
        .groupby("ERROR_ID")["ROW_ID"]
        .apply(list)
        .reset_index()
    )

    # Ensure that you maintain the ROW_ID, and ERROR_ID column names which are shown above. They are keywords in this project.
    rule_context.push_type_2(
        table=CINdetails, columns=[CINreferralDate], row_df=df_CINDetails_issues
    )
    rule_context.push_type_2(
        table=ChildIdentifiers,
        columns=[PersonBirthDate, ExpectedPersonBirthDate],
        row_df=df_ChildIdentifiers_issues,
    )


def test_validate():
    # Create dummy data
    sample_CINDetails = pd.DataFrame(
        [
            {
                "LAchildID": "child1",
                "CINreferralDate": "26/04/2000",  # Pass birth less than 280 days before referral
            },
            {
                "LAchildID": "child2",
                "CINreferralDate": "27/06/1998",  # Fail, referral more than 280 days before birth
            },
            {
                "LAchildID": "child3",
                "CINreferralDate": "07/04/2000",  # Pass, expected birth less than 280 days before referral
            },
            {
                "LAchildID": "child4",
                "CINreferralDate": "07/02/1998",  # Fail, referral date more than 280 days before expected birth
            },
        ]
    )
    sample_ChildIdentifiers = pd.DataFrame(
        [
            {
                "LAchildID": "child1",  # Pass
                "PersonBirthDate": "26/05/2000",
                "ExpectedPersonBirthDate": pd.NA,
            },
            {
                "LAchildID": "child2",  # Fails
                "PersonBirthDate": "26/05/2000",
                "ExpectedPersonBirthDate": pd.NA,
            },
            {
                "LAchildID": "child3",  # Pass
                "PersonBirthDate": pd.NA,
                "ExpectedPersonBirthDate": "26/05/2000",
            },
            {
                "LAchildID": "child4",  # Fail
                "PersonBirthDate": pd.NA,
                "ExpectedPersonBirthDate": "26/05/2000",
            },
        ]
    )

    # Convert date columns to dates
    sample_CINDetails[CINreferralDate] = pd.to_datetime(
        sample_CINDetails[CINreferralDate], format="%d/%m/%Y", errors="coerce"
    )
    sample_ChildIdentifiers["PersonBirthDate"] = pd.to_datetime(
        sample_ChildIdentifiers["PersonBirthDate"], format="%d/%m/%Y", errors="coerce"
    )

    sample_ChildIdentifiers["ExpectedPersonBirthDate"] = pd.to_datetime(
        sample_ChildIdentifiers["ExpectedPersonBirthDate"],
        format="%d/%m/%Y",
        errors="coerce",
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

    # check that the right columns were returned. Replace PersonBirthDate  with a list of your columns.
    issue_columns = issues.columns

    assert issue_columns == [PersonBirthDate, ExpectedPersonBirthDate]

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
                    "child2",  # ChildID
                    # Referral date
                    pd.to_datetime("27/06/1998", format="%d/%m/%Y", errors="coerce"),
                    # Birth date
                    pd.to_datetime("26/05/2000", format="%d/%m/%Y", errors="coerce"),
                    # Expected birth date
                    pd.to_datetime(pd.NA, format="%d/%m/%Y", errors="coerce"),
                ),
                "ROW_ID": [1],
            },
            {
                "ERROR_ID": (
                    "child4",  # ChildID
                    # Referral date
                    pd.to_datetime("07/02/1998", format="%d/%m/%Y", errors="coerce"),
                    # Birth date
                    pd.to_datetime(pd.NA, format="%d/%m/%Y", errors="coerce"),
                    # Expected birth date
                    pd.to_datetime("26/05/2000", format="%d/%m/%Y", errors="coerce"),
                ),
                "ROW_ID": [3],
            },
        ]
    )
    print(expected_df)
    assert issue_rows.equals(expected_df)

    # Check that the rule definition is what you wrote in the context above.

    # replace 2885 with the rule code and put the appropriate message in its place too.
    assert result.definition.code == 8606
    assert (
        result.definition.message
        == "Child referral date is more than 40 weeks before DOB or expected DOB"
    )
