"""
Rule number: 8555Q
Module: CIN details
Rule details: If <PersonDeathDate> (N00108) is present, then the <CINreferralDate> (N00100) must be on or before the <PersonDeathDate> (N00108)
Rule message: Child cannot be referred after its recorded date of death

"""

from typing import Mapping

import pandas as pd

from cin_validator.rule_engine import CINTable, RuleContext, RuleType, rule_definition
from cin_validator.test_engine import run_rule
from cin_validator.utils import make_census_period

ChildIdentifiers = CINTable.ChildIdentifiers
PersonDeathDate = ChildIdentifiers.PersonDeathDate
LAchildID = ChildIdentifiers.LAchildID

CINdetails = CINTable.CINdetails
CINreferralDate = CINdetails.CINreferralDate
LAchildID = CINdetails.LAchildID


@rule_definition(
    code="8555Q",
    module=CINTable.CINdetails,
    rule_type=RuleType.QUERY,
    message="Child cannot be referred after its recorded date of death",
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

    # <CINreferralDate> (N00100) must be on or before the <PersonDeathDate> (N00108)

    # Remove rows with no death date
    df_ChildIdentifiers = df_ChildIdentifiers[
        df_ChildIdentifiers[PersonDeathDate].notna()
    ]

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

    rule_context.push_type_2(
        table=CINdetails, columns=[CINreferralDate], row_df=df_CINDetails_issues
    )
    rule_context.push_type_2(
        table=ChildIdentifiers,
        columns=[PersonDeathDate],
        row_df=df_ChildIdentifiers_issues,
    )


def test_validate():
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

    sample_CINDetails[CINreferralDate] = pd.to_datetime(
        sample_CINDetails[CINreferralDate], format="%d/%m/%Y", errors="coerce"
    )
    sample_ChildIdentifiers["PersonDeathDate"] = pd.to_datetime(
        sample_ChildIdentifiers["PersonDeathDate"], format="%d/%m/%Y", errors="coerce"
    )

    result = run_rule(
        validate,
        {
            CINdetails: sample_CINDetails,
            ChildIdentifiers: sample_ChildIdentifiers,
        },
    )

    issues_list = result.type2_issues
    assert len(issues_list) == 2
    issues = issues_list[1]

    issue_table = issues.table
    assert issue_table == ChildIdentifiers

    issue_columns = issues.columns
    assert issue_columns == [PersonDeathDate]

    issue_rows = issues.row_df
    assert len(issue_rows) == 1
    assert isinstance(issue_rows, pd.DataFrame)
    assert issue_rows.columns.to_list() == ["ERROR_ID", "ROW_ID"]

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

    assert result.definition.code == "8555Q"
    assert (
        result.definition.message
        == "Child cannot be referred after its recorded date of death"
    )
