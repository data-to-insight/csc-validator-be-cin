from typing import Mapping

import pandas as pd

from cin_validator.rule_engine import CINTable, RuleContext, rule_definition
from cin_validator.test_engine import run_rule

CINdetails = CINTable.CINdetails
CINreferralDate = CINdetails.CINreferralDate
CINclosureDate = CINdetails.CINclosureDate
LAchildID = CINdetails.LAchildID


# define characteristics of rule
@rule_definition(
    code="8630",
    module=CINTable.CINdetails,
    message="CIN Closure Date is before CIN Referral Date for the same CIN episode",
    affected_fields=[CINreferralDate, CINclosureDate],
)
def validate(
    data_container: Mapping[CINTable, pd.DataFrame], rule_context: RuleContext
):
    df = data_container[CINdetails]

    # If present <CINclosureDate> (N00102) must be on or after <CINreferralDate> (N00100) in the same <CINdetails> instance

    df.index.name = "ROW_ID"

    df.query(
        "(CINclosureDate < CINreferralDate) and CINclosureDate.notna() and CINreferralDate.notna()",
        inplace=True,
    )

    df_issues = df.reset_index()

    link_id = tuple(
        zip(
            df_issues[LAchildID],
            df_issues[CINreferralDate],
            df_issues[CINclosureDate],
        )
    )
    df_issues["ERROR_ID"] = link_id
    df_issues = (
        df_issues.groupby("ERROR_ID", group_keys=False)["ROW_ID"]
        .apply(list)
        .reset_index()
    )

    rule_context.push_type_1(
        table=CINdetails,
        columns=[CINreferralDate, CINclosureDate],
        row_df=df_issues,
    )


def test_validate():
    IDS_are = [
        "AAAAAAAA",
        "BBBBBBBBB",
        "CCCCCCCCCCC",
        "DDDDDDDDD",
        "EEEE",
        "FFFFFFFFF",
        "GGGGGGGGGG",
        "HHHH",
    ]
    RefStart = [
        "01-01-2020",
        "01-02-2020",
        "01-03-2020",
        "15-01-2020",
        pd.NA,
        "01-07-2020",
        "15-01-2020",
        pd.NA,
    ]
    CINClose = [
        "01-01-2020",
        "01-01-2020",  #  Fails as Close before RefStart
        "01-03-2020",
        "17-01-2020",
        pd.NA,
        "01-01-2020",  #  Fails as Close before RefStart
        "15-01-2020",
        "01-01-2020",
    ]
    fake_dataframe = pd.DataFrame(
        {
            LAchildID: IDS_are,
            CINreferralDate: RefStart,
            CINclosureDate: CINClose,
        }
    )

    fake_dataframe[CINreferralDate] = pd.to_datetime(
        fake_dataframe[CINreferralDate], format=r"%d-%m-%Y", errors="coerce"
    )
    fake_dataframe[CINclosureDate] = pd.to_datetime(
        fake_dataframe[CINclosureDate], format=r"%d-%m-%Y", errors="coerce"
    )

    result = run_rule(validate, {CINdetails: fake_dataframe})

    issues = result.type1_issues

    issue_table = issues.table
    assert issue_table == CINdetails

    issue_columns = issues.columns
    assert issue_columns == [CINreferralDate, CINclosureDate]

    issue_rows = issues.row_df

    assert len(issue_rows) == 2

    assert isinstance(issue_rows, pd.DataFrame)
    assert issue_rows.columns.to_list() == ["ERROR_ID", "ROW_ID"]

    expected_df = pd.DataFrame(
        [
            {
                "ERROR_ID": (
                    "BBBBBBBBB",
                    pd.to_datetime("01-02-2020", format=r"%d-%m-%Y", errors="coerce"),
                    pd.to_datetime("01-01-2020", format=r"%d-%m-%Y", errors="coerce"),
                ),
                "ROW_ID": [1],
            },
            {
                "ERROR_ID": (
                    "FFFFFFFFF",
                    pd.to_datetime("01-07-2020", format=r"%d-%m-%Y", errors="coerce"),
                    pd.to_datetime("01-01-2020", format=r"%d-%m-%Y", errors="coerce"),
                ),
                "ROW_ID": [5],
            },
        ]
    )
    assert issue_rows.equals(expected_df)

    assert result.definition.code == "8630"
    assert (
        result.definition.message
        == "CIN Closure Date is before CIN Referral Date for the same CIN episode"
    )
