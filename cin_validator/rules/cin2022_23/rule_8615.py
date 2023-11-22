from typing import Mapping

import pandas as pd

from cin_validator.rule_engine import CINTable, RuleContext, rule_definition
from cin_validator.test_engine import run_rule

Section47 = CINTable.Section47
S47ActualStartDate = Section47.S47ActualStartDate
DateOfInitialCPC = Section47.DateOfInitialCPC
LAchildID = Section47.LAchildID


# define characteristics of rule
@rule_definition(
    code="8615",
    module=CINTable.Section47,
    message="Section 47 Enquiry Start Date must be present and cannot be later than the date of the initial Child Protection Conference",
    affected_fields=[S47ActualStartDate, DateOfInitialCPC],
)
def validate(
    data_container: Mapping[CINTable, pd.DataFrame], rule_context: RuleContext
):
    df = data_container[Section47]

    # Within a <Section47> group, if <DateOfInitialCPC> (N00110) is present then <S47ActualStartDate> (N00148) must be present and on or before the <DateOfInitialCPC> (N00110)
    df.index.name = "ROW_ID"

    df.query(
        "(S47ActualStartDate > DateOfInitialCPC) or (DateOfInitialCPC.notna() and S47ActualStartDate.isna())",
        inplace=True,
    )

    df_issues = df.reset_index()

    link_id = tuple(
        zip(
            df_issues[LAchildID],
            df_issues[S47ActualStartDate],
            df_issues[DateOfInitialCPC],
        )
    )
    df_issues["ERROR_ID"] = link_id
    df_issues = (
        df_issues.groupby("ERROR_ID", group_keys=False)["ROW_ID"]
        .apply(list)
        .reset_index()
    )

    rule_context.push_type_1(
        table=Section47,
        columns=[S47ActualStartDate, DateOfInitialCPC],
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
    S47start = [
        "01-01-2020",
        "01-02-2020",
        "01-03-2020",
        "15-01-2020",
        pd.NA,
        "01-07-2020",
        "15-01-2020",
        pd.NA,
    ]
    ICPCstart = [
        "01-01-2020",
        "01-01-2020",  #  Fails as ICPC before S47.
        "01-03-2020",
        "17-01-2020",
        pd.NA,
        "01-01-2020",  #  Fails as ICPC before S47.
        "15-01-2020",
        "01-01-2020",  #  Fails as ICPC with no S47.
    ]
    fake_dataframe = pd.DataFrame(
        {
            "LAchildID": IDS_are,
            "S47ActualStartDate": S47start,
            "DateOfInitialCPC": ICPCstart,
        }
    )

    fake_dataframe["S47ActualStartDate"] = pd.to_datetime(
        fake_dataframe["S47ActualStartDate"], format=r"%d-%m-%Y", errors="coerce"
    )
    fake_dataframe["DateOfInitialCPC"] = pd.to_datetime(
        fake_dataframe["DateOfInitialCPC"], format=r"%d-%m-%Y", errors="coerce"
    )

    result = run_rule(validate, {Section47: fake_dataframe})

    issues = result.type1_issues

    issue_table = issues.table
    assert issue_table == Section47

    issue_columns = issues.columns
    assert issue_columns == [S47ActualStartDate, DateOfInitialCPC]

    issue_rows = issues.row_df

    assert len(issue_rows) == 3

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
            {
                "ERROR_ID": (
                    "HHHH",
                    pd.NaT,
                    pd.to_datetime("01-01-2020", format=r"%d-%m-%Y", errors="coerce"),
                ),
                "ROW_ID": [7],
            },
        ]
    )
    assert issue_rows.equals(expected_df)

    assert result.definition.code == "8615"
    assert (
        result.definition.message
        == "Section 47 Enquiry Start Date must be present and cannot be later than the date of the initial Child Protection Conference"
    )
