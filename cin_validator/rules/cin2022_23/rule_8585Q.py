"""
Rule number: '8585Q'
Module: CIN plan dates
Rule details: If <ReasonForClosure> (N00103) = RC2 (Died) then a valid <PersonDeathDate> (N00108) must be present.
Rule message: Please check: CIN episode shows Died as the Closure Reason, however child has no recorded Date of Death

"""

from typing import Mapping

import pandas as pd

from cin_validator.rule_engine import CINTable, RuleContext, RuleType, rule_definition
from cin_validator.rules.cin2022_23.rule_8925 import LAchildID
from cin_validator.test_engine import run_rule

CINdetails = CINTable.CINdetails
LAchildID = CINdetails.LAchildID
ReasonForClosure = CINdetails.ReasonForClosure

ChildIdentifiers = CINTable.ChildIdentifiers
LAchildID = ChildIdentifiers.LAchildID
PersonDeathDate = ChildIdentifiers.PersonDeathDate


@rule_definition(
    code="8585Q",
    module=CINTable.CINdetails,
    rule_type=RuleType.QUERY,
    message="Please check and either amend or provide a reason: CIN episode shows Died as the Closure Reason, however child has no recorded Date of Death",
    affected_fields=[
        ReasonForClosure,
        PersonDeathDate,
    ],
)
def validate(
    data_container: Mapping[CINTable, pd.DataFrame], rule_context: RuleContext
):
    df_CI = data_container[ChildIdentifiers].copy()
    df_CIN = data_container[CINdetails].copy()

    df_CI.index.name = "ROW_ID"
    df_CIN.index.name = "ROW_ID"

    df_CI.reset_index(inplace=True)
    df_CIN.reset_index(inplace=True)

    # <ReasonForClosure> (N00103) = RC2 (Died) then a valid <PersonDeathDate> (N00108) must be present.
    df = df_CI.merge(
        df_CIN,
        left_on=["LAchildID"],
        right_on=["LAchildID"],
        how="left",
        suffixes=("_CPP", "_CIN"),
    )

    df = df[df[ReasonForClosure] == "RC2"]
    df = df[df[PersonDeathDate].isna()].reset_index()

    df["ERROR_ID"] = tuple(
        zip(df[LAchildID], df[ReasonForClosure], df[PersonDeathDate])
    )

    df_CI_issues = (
        df_CI.merge(df, left_on="ROW_ID", right_on="ROW_ID_CPP")
        .groupby("ERROR_ID")["ROW_ID"]
        .apply(list)
        .reset_index()
    )

    df_CIN_issues = (
        df_CIN.merge(df, left_on="ROW_ID", right_on="ROW_ID_CIN")
        .groupby("ERROR_ID")["ROW_ID"]
        .apply(list)
        .reset_index()
    )

    rule_context.push_type_2(
        table=ChildIdentifiers, columns=[PersonDeathDate], row_df=df_CI_issues
    )
    rule_context.push_type_2(
        table=CINdetails, columns=[ReasonForClosure], row_df=df_CIN_issues
    )


def test_validate():
    sample_CIN = pd.DataFrame(
        [
            {
                "LAchildID": "child1",  # Pass, closed due to death and death date
                "ReasonForClosure": "RC2",
            },
            {
                "LAchildID": "child2",  # Pass, ReasonForClosure not death
                "ReasonForClosure": "abc",
            },
            {
                "LAchildID": "child3",  # Fail, no death date
                "ReasonForClosure": "RC2",
            },
        ]
    )

    sample_CI = pd.DataFrame(
        [
            {
                "LAchildID": "child1",
                "PersonDeathDate": "26/05/2000",  # Pass
            },
            {
                "LAchildID": "child2",
                "PersonDeathDate": "27/06/2002",  # Pass
            },
            {
                "LAchildID": "child3",
                "PersonDeathDate": pd.NA,  # Fail
            },
        ]
    )

    sample_CI[PersonDeathDate] = pd.to_datetime(
        sample_CI[PersonDeathDate], format="%d/%m/%Y", errors="coerce"
    )

    result = run_rule(
        validate,
        {
            ChildIdentifiers: sample_CI,
            CINdetails: sample_CIN,
        },
    )

    issues_list = result.type2_issues
    assert len(issues_list) == 2

    issues = issues_list[1]

    issue_table = issues.table
    assert issue_table == CINdetails

    issue_columns = issues.columns
    assert issue_columns == [ReasonForClosure]

    issue_rows = issues.row_df
    assert len(issue_rows) == 1
    assert isinstance(issue_rows, pd.DataFrame)
    assert issue_rows.columns.to_list() == ["ERROR_ID", "ROW_ID"]

    expected_df = pd.DataFrame(
        [
            {
                "ERROR_ID": (
                    # ChildID
                    "child3",
                    # Reason for closure
                    "RC2",
                    # Death date
                    pd.to_datetime(pd.NA, format="%d/%m/%Y", errors="coerce"),
                ),
                "ROW_ID": [2],
            },
        ]
    )
    assert issue_rows.equals(expected_df)

    assert result.definition.code == "8585Q"
    assert (
        result.definition.message
        == "Please check and either amend or provide a reason: CIN episode shows Died as the Closure Reason, however child has no recorded Date of Death"
    )
