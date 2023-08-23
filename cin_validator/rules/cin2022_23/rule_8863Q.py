# this is similar to rule 8890

from typing import Mapping

import pandas as pd

from cin_validator.rule_engine import CINTable, RuleContext, RuleType, rule_definition
from cin_validator.test_engine import run_rule
from cin_validator.utils import make_census_period

# Get tables and columns of interest from the CINTable object defined in rule_engine/__api.py

Assessments = CINTable.Assessments
AssessmentActualStartDate = Assessments.AssessmentActualStartDate
AssessmentAuthorisationDate = Assessments.AssessmentAuthorisationDate
LAchildID = Assessments.LAchildID
CINdetailsID = Assessments.CINdetailsID

Header = CINTable.Header
ReferenceDate = Header.ReferenceDate


# define characteristics of rule
@rule_definition(
    code="8863Q",
    module=CINTable.Assessments,
    rule_type=RuleType.QUERY,
    message="An Assessment is shown as starting when there is another Assessment ongoing.",
    affected_fields=[AssessmentActualStartDate, AssessmentAuthorisationDate],
)
def validate(
    data_container: Mapping[CINTable, pd.DataFrame], rule_context: RuleContext
):
    # PREPARING DATA

    # Name of the table required.
    df_ass = data_container[Assessments].copy()
    df_ass_2 = data_container[Assessments].copy()

    # Before you begin, rename the index and make it a column, so that the initial row positions can be kept intact.
    df_ass.index.name = "ROW_ID"
    df_ass_2.index.name = "ROW_ID"

    df_ass.reset_index(inplace=True)
    df_ass_2.reset_index(inplace=True)

    # ReferenceDate exists in the header table so we get header table too.
    df_ref = data_container[Header]
    ref_date_series = df_ref[ReferenceDate]

    # the make_census_period function generates the start and end date so that you don't have to do it each time.
    collection_start, reference_date = make_census_period(ref_date_series)

    # lOGIC
    # Implement rule logic as described by the Github issue.
    # Put the description as a comment above the implementation as shown.

    # Within one <CINdetails> group, where present each <AssessmentActualStartDate> (N00159) must not fall on or between:
    # a) the <AssessmentActualStartDate> (N00159) and <AssessmentAuthorisation Date (N00160) of any other <Assessments> group that has an <AssessmentAuthorisationDate> (N00160)
    # OR
    # b) the <AssessmentActualStartDate> (N00159) and the <ReferenceDate> (N00603) where the <AssessmentAuthorisationDate> (N00160) is missing

    df_ass = df_ass[df_ass[AssessmentActualStartDate].notna()]
    df_ass_2 = df_ass_2[df_ass_2[AssessmentActualStartDate].notna()]

    #  Merge tables to test for overlaps
    df_merged = df_ass.merge(
        df_ass_2,
        on=[LAchildID, CINdetailsID],
        how="left",
        suffixes=("_ass", "_ass2"),
    )

    # Prevent Assessments from being compared to themselves.
    same_start = (
        df_merged["AssessmentActualStartDate_ass"]
        == df_merged["AssessmentActualStartDate_ass2"]
    )
    same_end = (
        df_merged["AssessmentAuthorisationDate_ass"]
        == df_merged["AssessmentAuthorisationDate_ass2"]
    ) | (
        df_merged[
            "AssessmentAuthorisationDate_ass"
        ].isna()  # nans are checked separately because they are not considered equal by ==
        & df_merged["AssessmentAuthorisationDate_ass2"].isna()
    )
    duplicate = same_start & same_end
    df_merged = df_merged[~duplicate]

    # Determine whether assessment overlaps with another assessment
    ass_started_after_start = (
        df_merged["AssessmentActualStartDate_ass"]  # 1 starts later than 2 starts
        >= df_merged["AssessmentActualStartDate_ass2"]
    )
    ass_started_before_end = (
        df_merged["AssessmentActualStartDate_ass"]  # 1 starts earlier than 2 finishes
        <= df_merged["AssessmentAuthorisationDate_ass2"]
    ) & df_merged["AssessmentAuthorisationDate_ass2"].notna()
    ass_started_before_refdate = (
        df_merged["AssessmentActualStartDate_ass"] <= reference_date
    ) & df_merged["AssessmentAuthorisationDate_ass2"].isna()

    df_merged = df_merged[
        ass_started_after_start & (ass_started_before_end | ass_started_before_refdate)
    ].reset_index()

    # create an identifier for each error instance.
    df_merged["ERROR_ID"] = tuple(
        zip(
            df_merged[LAchildID],
            df_merged[CINdetailsID],
            df_merged["AssessmentActualStartDate_ass"],
        )
    )

    # we can now map the suffixes columns to their corresponding source tables such that the failing ROW_IDs and ERROR_IDs exist per table.
    df_ass_issues = (
        df_ass.merge(df_merged, left_on="ROW_ID", right_on="ROW_ID_ass")
        .groupby("ERROR_ID", group_keys=False)["ROW_ID"]
        .apply(list)
        .reset_index()
    )

    df_ass_2_issues = (
        df_ass_2.merge(df_merged, left_on="ROW_ID", right_on="ROW_ID_ass2")
        .groupby("ERROR_ID", group_keys=False)["ROW_ID"]
        .apply(list)
        .reset_index()
    )

    # Ensure that you maintain the ROW_ID, and ERROR_ID column names which are shown above. They are keywords in this project.
    rule_context.push_type_3(
        table=Assessments, columns=[AssessmentActualStartDate], row_df=df_ass_issues
    )
    rule_context.push_type_3(
        table=Assessments,
        columns=[AssessmentActualStartDate, AssessmentAuthorisationDate],
        row_df=df_ass_2_issues,
    )


def test_validate():
    # Create some sample data such that some values pass the validation and some fail.
    sample_header = pd.DataFrame(
        [{ReferenceDate: "31/03/2001"}]  # the census start date here will be 01/04/2000
    )

    sample_ass = pd.DataFrame(
        [  # child1
            {
                "LAchildID": "child1",
                "CINdetailsID": "cinID1",
                "AssessmentActualStartDate": "26/05/2000",  # 0 Pass: not between "26/08/2000" and "31/03/2001"
                "AssessmentAuthorisationDate": "26/10/2000",
            },
            {
                "LAchildID": "child1",
                "CINdetailsID": "cinID1",
                "AssessmentActualStartDate": "26/08/2000",  # 1 Fail: between "26/05/2000" and "26/10/2000"
                "AssessmentAuthorisationDate": pd.NA,
            },
            {
                "LAchildID": "child2",  # 2 alone in cin group: not compared
                "CINdetailsID": "cinID2",
                "AssessmentActualStartDate": "26/05/2000",
                "AssessmentAuthorisationDate": "25/10/2000",
            },
            {
                "LAchildID": "child2",  # 3 alone in cin group: not compared
                "CINdetailsID": "cinID22",
                "AssessmentActualStartDate": "26/10/2000",
                "AssessmentAuthorisationDate": "26/12/2000",
            },
            # child3
            {
                "LAchildID": "child3",
                "CINdetailsID": "cinID3",
                "AssessmentActualStartDate": "26/05/2000",  # 4 Pass: not between "26/08/2000" and "26/10/2000"
                "AssessmentAuthorisationDate": "26/10/2001",
            },
            {
                "LAchildID": "child3",
                "CINdetailsID": "cinID3",
                "AssessmentActualStartDate": "26/08/2000",  # 5 Fail: between "26/05/2000" and "26/10/2001"
                "AssessmentAuthorisationDate": "26/10/2000",
            },
            # child4
            {
                "LAchildID": "child4",
                "CINdetailsID": "cinID1",
                "AssessmentActualStartDate": "26/10/2000",  # 6 Fail: between "26/09/2000" and ReferenceDate
                "AssessmentAuthorisationDate": "31/03/2001",
            },
            {
                "LAchildID": "child4",
                "CINdetailsID": "cinID1",
                "AssessmentActualStartDate": "26/09/2000",  # 7 Pass: not between "26/10/2000" and "31/03/2001"
                "AssessmentAuthorisationDate": pd.NA,
            },
            {
                "LAchildID": "child5",
                "CINdetailsID": "cinID1",
                "AssessmentActualStartDate": "01/03/2000",
                "AssessmentAuthorisationDate": "01/04/2000",
            },
            {
                "LAchildID": "child5",
                "CINdetailsID": "cinID1",
                "AssessmentActualStartDate": "01/09/2000",
                "AssessmentAuthorisationDate": "01/10/2000",
            },
        ]
    )

    # If rule requires columns containing date values, convert those columns to datetime objects first. Do it here in the test_validate function, not above.
    sample_ass[AssessmentActualStartDate] = pd.to_datetime(
        sample_ass[AssessmentActualStartDate], format="%d/%m/%Y", errors="coerce"
    )
    sample_ass[AssessmentAuthorisationDate] = pd.to_datetime(
        sample_ass[AssessmentAuthorisationDate], format="%d/%m/%Y", errors="coerce"
    )
    sample_header[ReferenceDate] = pd.to_datetime(
        sample_header[ReferenceDate], format="%d/%m/%Y", errors="coerce"
    )

    # Run the rule function, passing in our sample data.
    result = run_rule(
        validate,
        {
            Assessments: sample_ass,
            Header: sample_header,
        },
    )

    issues_list = result.type3_issues
    assert len(issues_list) == 2
    # the function returns a list on NamedTuples where each NamedTuple contains (table, column_list, df_issues)
    # pick any table and check it's values. the tuple in location 1 will contain the Reviews columns because that's the second thing pushed above.
    issues = issues_list[0]

    # get table name and check it. Replace Reviews with the name of your table.
    issue_table = issues.table
    assert issue_table == Assessments

    issue_columns = issues.columns
    assert issue_columns == [AssessmentActualStartDate]

    # check that the location linking dataframe was formed properly.
    issue_rows = issues.row_df
    # replace 3 with the number of failing points you expect from the sample data.
    assert len(issue_rows) == 3

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
                    "child1",
                    "cinID1",
                    pd.to_datetime("26/08/2000", format="%d/%m/%Y", errors="coerce"),
                ),
                "ROW_ID": [1],
            },
            {
                "ERROR_ID": (
                    "child3",
                    "cinID3",
                    pd.to_datetime("26/08/2000", format="%d/%m/%Y", errors="coerce"),
                ),
                "ROW_ID": [5],
            },
            {
                "ERROR_ID": (
                    "child4",
                    "cinID1",
                    pd.to_datetime("26/10/2000", format="%d/%m/%Y", errors="coerce"),
                ),
                "ROW_ID": [6],
            },
        ]
    )
    assert issue_rows.equals(expected_df)

    # Check that the rule definition is what you wrote in the context above.

    # replace 8863Q with the rule code and put the appropriate message in its place too.
    assert result.definition.code == "8863Q"
    assert (
        result.definition.message
        == "An Assessment is shown as starting when there is another Assessment ongoing."
    )
