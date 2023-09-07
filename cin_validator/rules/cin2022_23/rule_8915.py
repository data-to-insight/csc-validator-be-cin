"""
Rule number: '8915'
Module: Child protection plans
Rule details: If <PersonDeathDate> (N00108) is present, then <CPPstartDate> (N00105) must be on or before <PersonDeathDate> (N00108)
Rule message: Child Protection Plan shown as starting after the child’s Date of Death

"""
from typing import Mapping

import pandas as pd

from cin_validator.rule_engine import CINTable, RuleContext, rule_definition
from cin_validator.test_engine import run_rule

# Get tables and columns of interest from the CINTable object defined in rule_engine/__api.py

ChildProtectionPlans = CINTable.ChildProtectionPlans
CPPstartDate = ChildProtectionPlans.CPPstartDate
CPP_LAID = ChildProtectionPlans.LAchildID

ChildIdentifiers = CINTable.ChildIdentifiers
PersonDeathDate = ChildIdentifiers.PersonDeathDate
CI_LAID = ChildIdentifiers.LAchildID


# define characteristics of rule
@rule_definition(
    code="8915",
    module=CINTable.ChildProtectionPlans,
    message="Child Protection Plan shown as starting after the child’s Date of Death",
    affected_fields=[
        CPPstartDate,
        PersonDeathDate,
    ],
)
def validate(
    data_container: Mapping[CINTable, pd.DataFrame], rule_context: RuleContext
):
    df_CPP = data_container[ChildProtectionPlans].copy()
    df_CI = data_container[ChildIdentifiers].copy()

    df_CPP.index.name = "ROW_ID"
    df_CI.index.name = "ROW_ID"

    df_CPP.reset_index(inplace=True)
    df_CI.reset_index(inplace=True)

    # Remove rows with no death date

    df_CI = df_CI[df_CI[PersonDeathDate].notna()]

    # <CPPstartDate> (N00105) must be on or before <PersonDeathDate>

    # Join 2 tables together

    df = df_CPP.merge(
        df_CI,
        left_on=["LAchildID"],
        right_on=["LAchildID"],
        how="left",
        suffixes=("_CPP", "_CI"),
    )

    # Return those where dates don't align
    df = df[df[CPPstartDate] > df[PersonDeathDate]].reset_index()

    df["ERROR_ID"] = tuple(zip(df["LAchildID"], df[CPPstartDate], df[PersonDeathDate]))

    df_CPP_issues = (
        df_CPP.merge(df, left_on="ROW_ID", right_on="ROW_ID_CPP")
        .groupby("ERROR_ID", group_keys=False)["ROW_ID"]
        .apply(list)
        .reset_index()
    )

    df_CI_issues = (
        df_CI.merge(df, left_on="ROW_ID", right_on="ROW_ID_CI")
        .groupby("ERROR_ID", group_keys=False)["ROW_ID"]
        .apply(list)
        .reset_index()
    )

    rule_context.push_type_2(
        table=ChildProtectionPlans, columns=[CPPstartDate], row_df=df_CPP_issues
    )
    rule_context.push_type_2(
        table=ChildIdentifiers, columns=[PersonDeathDate], row_df=df_CI_issues
    )


def test_validate():
    # Create some sample data such that some values pass the validation and some fail.
    sample_CPP = pd.DataFrame(
        [
            {
                "LAchildID": "child1",
                "CPPstartDate": pd.NA,  # Pass, no CP plan
            },
            {
                "LAchildID": "child2",
                "CPPstartDate": "27/06/2002",  # Fail, after death
            },
            {
                "LAchildID": "child3",
                "CPPstartDate": "07/02/1999",  # Pass, prior to death
            },
            {
                "LAchildID": "child4",
                "CPPstartDate": "26/05/2000",  # Pass, no death
            },
            {
                "LAchildID": "child5",
                "CPPstartDate": "26/05/2001",  # Fail, after death
            },
        ]
    )

    sample_CI = pd.DataFrame(
        [
            {
                "LAchildID": "child1",  # Pass
                "PersonDeathDate": "26/05/2000",
            },
            {
                "LAchildID": "child2",  # Fail
                "PersonDeathDate": "26/05/2000",
            },
            {
                "LAchildID": "child3",  # Pass
                "PersonDeathDate": "26/05/2000",
            },
            {
                "LAchildID": "child4",  # Pass
                "PersonDeathDate": pd.NA,
            },
            {
                "LAchildID": "child5",  # Fail
                "PersonDeathDate": "27/05/2000",
            },
        ]
    )

    sample_CPP[CPPstartDate] = pd.to_datetime(
        sample_CPP[CPPstartDate], format="%d/%m/%Y", errors="coerce"
    )
    sample_CI[PersonDeathDate] = pd.to_datetime(
        sample_CI[PersonDeathDate], format="%d/%m/%Y", errors="coerce"
    )

    # Run rule function passing in our sample data
    result = run_rule(
        validate,
        {
            ChildProtectionPlans: sample_CPP,
            ChildIdentifiers: sample_CI,
        },
    )

    # The result contains a list of issues encountered
    issues_list = result.type2_issues
    assert len(issues_list) == 2

    issues = issues_list[1]

    issue_table = issues.table
    assert issue_table == ChildIdentifiers

    # check that the right columns were returned. Replace PersonDeathDate  with a list of your columns.
    issue_columns = issues.columns
    assert issue_columns == [PersonDeathDate]

    # check that the location linking dataframe was formed properly.
    issue_rows = issues.row_df
    # replace 2 with the number of failing points you expect from the sample data.
    assert len(issue_rows) == 2
    # check that the failing locations are contained in a DataFrame having the appropriate columns. These lines do not change.
    assert isinstance(issue_rows, pd.DataFrame)
    assert issue_rows.columns.to_list() == ["ERROR_ID", "ROW_ID"]

    expected_df = pd.DataFrame(
        [
            {
                "ERROR_ID": (
                    "child2",  # ChildID
                    # Start date
                    pd.to_datetime("27/06/2002", format="%d/%m/%Y", errors="coerce"),
                    # Referral date
                    pd.to_datetime("26/05/2000", format="%d/%m/%Y", errors="coerce"),
                ),
                "ROW_ID": [1],
            },
            {
                "ERROR_ID": (
                    "child5",  # ChildID
                    # Start Date
                    pd.to_datetime("26/05/2001", format="%d/%m/%Y", errors="coerce"),
                    # Referral date
                    pd.to_datetime("27/05/2000", format="%d/%m/%Y", errors="coerce"),
                ),
                "ROW_ID": [4],
            },
        ]
    )

    assert issue_rows.equals(expected_df)

    assert result.definition.code == "8915"
    assert (
        result.definition.message
        == "Child Protection Plan shown as starting after the child’s Date of Death"
    )
