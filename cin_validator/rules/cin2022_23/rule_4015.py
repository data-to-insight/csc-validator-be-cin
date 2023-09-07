from typing import Mapping

import pandas as pd

from cin_validator.rule_engine import CINTable, RuleContext, rule_definition
from cin_validator.test_engine import run_rule
from cin_validator.utils import make_census_period

CINplanDates = CINTable.CINplanDates
CINPlanStartDate = CINplanDates.CINPlanStartDate
LAchildID = CINplanDates.LAchildID

CINdetails = CINTable.CINdetails
CINreferralDate = CINdetails.CINreferralDate


@rule_definition(
    code="4015",
    module=CINTable.CINplanDates,
    message="The CIN Plan start date cannot be before the referral date",
    affected_fields=[
        CINPlanStartDate,
        CINreferralDate,
    ],
)
def validate(
    data_container: Mapping[CINTable, pd.DataFrame], rule_context: RuleContext
):
    """
    Where present, the <CINPlanStartDate> (N00689) must be on or after the <CINReferralDate> (N00100)
    """
    df_cindetail = data_container[CINdetails].copy()
    df_cinplan = data_container[CINplanDates].copy()

    df_cindetail.index.name = "ROW_ID"
    df_cinplan.index.name = "ROW_ID"

    df_cindetail.reset_index(inplace=True)
    df_cinplan.reset_index(inplace=True)

    # Where present, the <CINPlanStartDate> (N00689) must be on or after the <CINReferralDate> (N00100)
    df_cinplan = df_cinplan[df_cinplan[CINPlanStartDate].notna()]

    df_merged = df_cindetail.merge(
        df_cinplan,
        how="inner",
        on=["LAchildID", "CINdetailsID"],
        suffixes=["_det", "_plan"],
    )

    df_merged.query(r"CINPlanStartDate < CINreferralDate", inplace=True)

    df_merged["ERROR_ID"] = tuple(
        zip(
            df_merged[LAchildID],
            df_merged[CINPlanStartDate],
            df_merged[CINreferralDate],
        )
    )

    df_cindet_issues = (
        df_cinplan.merge(
            df_merged, how="inner", left_on="ROW_ID", right_on="ROW_ID_plan"
        )
        .groupby("ERROR_ID", group_keys=False)["ROW_ID"]
        .apply(list)
        .reset_index()
    )

    df_cinplan_issues = (
        df_cindetail.merge(
            df_merged, how="inner", left_on="ROW_ID", right_on="ROW_ID_det"
        )
        .groupby("ERROR_ID", group_keys=False)["ROW_ID"]
        .apply(list)
        .reset_index()
    )

    rule_context.push_type_2(
        table=CINplanDates, columns=[CINPlanStartDate], row_df=df_cindet_issues
    )
    rule_context.push_type_2(
        table=CINdetails, columns=[CINreferralDate], row_df=df_cinplan_issues
    )


def test_validate():
    plan_is = (
        # ID     #CINID   #PlanStartDate
        ("1", "45", "2020-05-05"),  # 0
        ("4", "55", "2019-04-20"),  # 1
        ("67", "66", "2014-03-21"),  # 2
        ("69", "67", "2018-04-20"),  # 3
        ("69", "67", pd.NA),  # 4
        ("167", "166", "2014-03-21"),  # 5
    )

    cin_is = (
        # ID     #CINID   #CIN Ref Date
        ("1", "44", "2017-05-05"),  # A  0
        ("4", "55", "2019-04-20"),  # B  1
        ("67", "66", "2016-03-21"),  # C  2
        ("67", "67", "2015-03-21"),  # D  3
        ("69", "67", "2018-04-20"),  # E  4
        ("70", "69", "2015-04-20"),  # F  5
        ("167", "166", "2015-02-21"),  # G  6
    )

    fake_cinplan = pd.DataFrame(
        {
            "LAchildID": [x[0] for x in plan_is],
            "CINdetailsID": [x[1] for x in plan_is],
            "CINPlanStartDate": [x[2] for x in plan_is],
        }
    )
    fake_cindetail = pd.DataFrame(
        {
            "LAchildID": [x[0] for x in cin_is],
            "CINdetailsID": [x[1] for x in cin_is],
            "CINreferralDate": [x[2] for x in cin_is],
        }
    )
    fake_cinplan[CINPlanStartDate] = pd.to_datetime(
        fake_cinplan[CINPlanStartDate], format=r"%Y-%m-%d", errors="coerce"
    )
    fake_cindetail["CINreferralDate"] = pd.to_datetime(
        fake_cindetail["CINreferralDate"], format=r"%Y-%m-%d", errors="coerce"
    )

    result = run_rule(
        validate,
        {
            CINplanDates: fake_cinplan,
            CINdetails: fake_cindetail,
        },
    )

    issues_list = result.type2_issues

    assert len(issues_list) == 2
    issues = issues_list[1]

    issue_table = issues.table
    assert issue_table == CINdetails

    issue_columns = issues.columns
    assert issue_columns == [CINreferralDate]

    issue_rows = issues.row_df

    assert len(issue_rows) == 2

    assert isinstance(issue_rows, pd.DataFrame)
    assert issue_rows.columns.to_list() == ["ERROR_ID", "ROW_ID"]

    expected_df = pd.DataFrame(
        [
            {
                "ERROR_ID": (
                    "67",  # ChildID
                    # Start Date
                    pd.to_datetime("21/03/2014", format=r"%d/%m/%Y", errors="coerce"),
                    # Ref date
                    pd.to_datetime("21/03/2016", format=r"%d/%m/%Y", errors="coerce"),
                ),
                "ROW_ID": [2],
            },
            {
                "ERROR_ID": (
                    "167",  # ChildID
                    # Start date
                    pd.to_datetime("21/03/2014", format=r"%d/%m/%Y", errors="coerce"),
                    # Ref date
                    pd.to_datetime("21/02/2015", format=r"%d/%m/%Y", errors="coerce"),
                ),
                "ROW_ID": [6],
            },
        ]
    )
    issue_rows.sort_values(["ROW_ID"], ignore_index=True, inplace=True)
    assert issue_rows.equals(expected_df)

    assert result.definition.code == "4015"
    assert (
        result.definition.message
        == "The CIN Plan start date cannot be before the referral date"
    )
