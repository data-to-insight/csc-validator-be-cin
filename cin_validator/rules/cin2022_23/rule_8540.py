from typing import Mapping

import pandas as pd

from cin_validator.rule_engine import CINTable, RuleContext, rule_definition
from cin_validator.test_engine import run_rule

ChildIdentifiers = CINTable.ChildIdentifiers
CINdetails = CINTable.CINdetails
Disabilities = CINTable.Disabilities

LAchildID = ChildIdentifiers.LAchildID
PersonBirthDate = ChildIdentifiers.PersonBirthDate
ReferralNFA = CINdetails.ReferralNFA
Disability = Disabilities.Disability


@rule_definition(
    code="8540",
    module=CINTable.ChildCharacteristics,
    message="Child’s disability is missing or invalid (see Disability table)",
    affected_fields=[Disability, PersonBirthDate, ReferralNFA],
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

    df_ci_cin = df_ci.merge(
        df_cin, on="LAchildID", how="left", suffixes=("_ci", "_cin")
    )
    merged_df = df_ci_cin.merge(
        df_dis, on="LAchildID", how="left", suffixes=("", "_dis")
    )

    merged_df = merged_df[~merged_df[Disability].isin(valid_dis)]
    merged_df = merged_df[merged_df[PersonBirthDate].notna()]
    merged_df = merged_df[merged_df[ReferralNFA].isin(["false", "0"])]

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
                "LAchildID": "child5",  # 4 ignore: has no ReferralNFA
                "PersonBirthDate": "01/01/1880",
            },
        ]
    )

    sample_cin = pd.DataFrame(
        [
            {
                "LAchildID": "child1",  # considered
                "CINdetailsID": "CINID1",
                "ReferralNFA": "false",
            },
            {
                "LAchildID": "child2",  # considered
                "CINdetailsID": "CINID1",
                "ReferralNFA": "0",
            },
            {
                "LAchildID": "child3",  # considered since one of its ReferralNFA values is false/0
                "CINdetailsID": "CINID1",
                "ReferralNFA": "0",
            },
            {
                "LAchildID": "child3",
                "CINdetailsID": "CINID2",
                "ReferralNFA": "1",
            },
            {
                "LAchildID": "child4",  # 3 ignore ReferralNFA is not false/0
                "CINdetailsID": "CINID1",
                "ReferralNFA": "1",
            },
        ]
    )

    sample_dis = pd.DataFrame(
        [
            {
                "LAchildID": "child1",  # 0 fail: Disability should be present
                "Disability": pd.NA,
            },
            {
                "LAchildID": "child2",  # 1 pass
                "Disability": "MOB",
            },
            {
                "LAchildID": "child3",  # 2 fail: Disability should be valid
                "Disability": "notreal",
            },
        ]
    )

    result = run_rule(
        validate,
        {
            ChildIdentifiers: sample_ci,
            Disabilities: sample_dis,
            CINdetails: sample_cin,
        },
    )

    issues_list = result.type2_issues
    assert len(issues_list) == 3
    issues = issues_list[1]

    issue_table = issues.table
    assert issue_table == Disabilities

    issue_columns = issues.columns
    assert issue_columns == [Disability]

    issue_rows = issues.row_df
    assert len(issue_rows) == 2
    assert isinstance(issue_rows, pd.DataFrame)
    assert issue_rows.columns.to_list() == ["ERROR_ID", "ROW_ID"]

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
                    "child3",
                    "01/01/1880",
                ),
                "ROW_ID": [2],
            },
        ]
    )
    assert issue_rows.equals(expected_df)

    assert result.definition.code == "8540"
    assert (
        result.definition.message
        == "Child’s disability is missing or invalid (see Disability table)"
    )
