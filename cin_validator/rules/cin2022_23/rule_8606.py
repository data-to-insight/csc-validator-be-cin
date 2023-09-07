"""
Rule number: '8606'
Module: CIN details
Rule details: <CINreferralDate> (N00100) cannot be more than 280 days before <PersonBirthDate> (N00066) or <ExpectedPersonBirthDate> (N00098)
Rule message: Child referral date is more than 40 weeks before DOB or expected DOB

"""

import datetime as dt
from typing import Mapping

import pandas as pd

from cin_validator.rule_engine import CINTable, RuleContext, rule_definition
from cin_validator.test_engine import run_rule

ChildIdentifiers = CINTable.ChildIdentifiers
PersonBirthDate = ChildIdentifiers.PersonBirthDate
ExpectedPersonBirthDate = ChildIdentifiers.ExpectedPersonBirthDate
LAchildID = ChildIdentifiers.LAchildID

CINdetails = CINTable.CINdetails
CINreferralDate = CINdetails.CINreferralDate
LAchildID = CINdetails.LAchildID


@rule_definition(
    code="8606",
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
    df_merged = df_CINDetails.merge(
        df_ChildIdentifiers,
        left_on=["LAchildID"],
        right_on=["LAchildID"],
        how="left",
        suffixes=("_CINDetails", "_ChildIdentifiers"),
    )

    # Get rows where CINreferralDate is earlier than birth/expected birth -280
    condition1 = df_merged[CINreferralDate] < (
        df_merged[PersonBirthDate] - dt.timedelta(days=280)
    )
    condition2 = df_merged[CINreferralDate] < (
        df_merged[ExpectedPersonBirthDate] - dt.timedelta(days=280)
    )
    df_merged = df_merged[condition1 | condition2].reset_index()

    df_merged["ERROR_ID"] = tuple(
        zip(
            df_merged[LAchildID],
            df_merged[CINreferralDate],
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
        columns=[PersonBirthDate, ExpectedPersonBirthDate],
        row_df=df_ChildIdentifiers_issues,
    )


def test_validate():
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

    assert issue_columns == [PersonBirthDate, ExpectedPersonBirthDate]

    issue_rows = issues.row_df

    assert len(issue_rows) == 2
    assert isinstance(issue_rows, pd.DataFrame)
    assert issue_rows.columns.to_list() == ["ERROR_ID", "ROW_ID"]

    expected_df = pd.DataFrame(
        [
            {
                "ERROR_ID": (
                    "child2",  # ChildID
                    # Referral date
                    pd.to_datetime("27/06/1998", format="%d/%m/%Y", errors="coerce"),
                ),
                "ROW_ID": [1],
            },
            {
                "ERROR_ID": (
                    "child4",  # ChildID
                    # Referral date
                    pd.to_datetime("07/02/1998", format="%d/%m/%Y", errors="coerce"),
                ),
                "ROW_ID": [3],
            },
        ]
    )

    assert issue_rows.equals(expected_df)

    assert result.definition.code == "8606"
    assert (
        result.definition.message
        == "Child referral date is more than 40 weeks before DOB or expected DOB"
    )
