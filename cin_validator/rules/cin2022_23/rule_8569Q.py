from typing import Mapping

import pandas as pd

from cin_validator.rule_engine import CINTable, RuleContext, RuleType, rule_definition
from cin_validator.test_engine import run_rule
from cin_validator.utils import england_working_days, make_census_period

CINdetails = CINTable.CINdetails

ReferralNFA = CINdetails.ReferralNFA
LAchildID = CINdetails.LAchildID
CINdetailsID = CINdetails.CINdetailsID
CINreferralDate = CINdetails.CINreferralDate

Header = CINTable.Header
ReferenceDate = Header.ReferenceDate


@rule_definition(
    code="8569Q",
    module=CINTable.CINdetails,
    rule_type=RuleType.QUERY,
    message="A case with referral date before one working day prior to the collection start date must not be flagged as a no further action case",
    affected_fields=[
        ReferralNFA,
    ],
)
def validate(
    data_container: Mapping[CINTable, pd.DataFrame], rule_context: RuleContext
):
    df_cin = data_container[CINdetails].copy()
    df_cin.index.name = "ROW_ID"

    df_cin.reset_index(inplace=True)

    header = data_container[Header]
    ref_date_series = header[ReferenceDate]
    collection_start, collection_end = make_census_period(ref_date_series)

    # If <CINreferralDate> (N00100) is before [Start_of_Census_Year] minus 1 working day, <ReferralNFA> (N00112) must be false
    df_cin_issues = df_cin[
        df_cin[CINreferralDate] < (collection_start - england_working_days(1))
    ]

    df_cin_issues = df_cin_issues[
        ~df_cin_issues[ReferralNFA].isin(["false", "0"])
    ].reset_index()

    df_cin_issues["ERROR_ID"] = tuple(
        zip(
            df_cin_issues[LAchildID],
            df_cin_issues[CINdetailsID],
            df_cin_issues[CINreferralDate],
        )
    )

    df_issues = (
        df_cin_issues.groupby("ERROR_ID", group_keys=False)["ROW_ID"]
        .apply(list)
        .reset_index()
    )

    rule_context.push_type_2(
        table=CINdetails, columns=[CINreferralDate], row_df=df_issues
    )


def test_validate():
    sample_cin_details = pd.DataFrame(
        [
            {
                "LAchildID": "child1",
                "CINreferralDate": "26/10/1880",
                "ReferralNFA": "true",
                "CINdetailsID": "cinID1",
            },
            {
                "LAchildID": "child2",
                "CINreferralDate": "26/10/2001",
                "ReferralNFA": "true",
                "CINdetailsID": "cinID2",
            },
            {
                "LAchildID": "child3",
                "CINreferralDate": "26/10/1880",
                "ReferralNFA": "false",
                "CINdetailsID": "cinID3",
            },
            {
                "LAchildID": "child4",
                "CINreferralDate": "26/10/1999",
                "ReferralNFA": "false",
                "CINdetailsID": "cinID4",
            },
            {
                "LAchildID": "child5",
                "CINreferralDate": pd.NA,
                "ReferralNFA": "false",
                "CINdetailsID": "cinID5",
            },
        ]
    )
    sample_cin_details["CINreferralDate"] = pd.to_datetime(
        sample_cin_details["CINreferralDate"], format="%d/%m/%Y", errors="coerce"
    )
    sample_header = pd.DataFrame(
        [{ReferenceDate: "31/03/2000"}]  # the census start date here will be 01/04/2000
    )

    result = run_rule(
        validate,
        {
            CINdetails: sample_cin_details,
            Header: sample_header,
        },
    )

    issues_list = result.type2_issues
    assert len(issues_list) == 1
    issues = issues_list[0]

    issue_table = issues.table
    assert issue_table == CINdetails

    issue_columns = issues.columns
    assert issue_columns == [CINreferralDate]

    issue_rows = issues.row_df
    assert len(issue_rows) == 1
    assert isinstance(issue_rows, pd.DataFrame)
    assert issue_rows.columns.to_list() == ["ERROR_ID", "ROW_ID"]

    expected_df = pd.DataFrame(
        [
            {
                "ERROR_ID": (
                    "child1",  # ChildID
                    "cinID1",  # CINdetailsID
                    # corresponding CPPstartDate
                    pd.to_datetime("26/10/1880", format="%d/%m/%Y", errors="coerce"),
                ),
                "ROW_ID": [0],
            },
        ]
    )
    assert issue_rows.equals(expected_df)

    assert result.definition.code == "8569Q"
    assert (
        result.definition.message
        == "A case with referral date before one working day prior to the collection start date must not be flagged as a no further action case"
    )
