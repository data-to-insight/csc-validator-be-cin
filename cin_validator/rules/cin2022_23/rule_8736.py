from typing import Mapping

import pandas as pd

from cin_validator.rule_engine import CINTable, RuleContext, rule_definition
from cin_validator.test_engine import run_rule
from cin_validator.utils import make_census_period

# Get tables and columns of interest from the CINTable object defined in rule_engine/__api.py

Assessments = CINTable.Assessments
AssessmentAuthorisationDate = Assessments.AssessmentAuthorisationDate
AssessmentActualStartDate = Assessments.AssessmentActualStartDate
CINdetailsID = Assessments.CINdetailsID
LAchildID = Assessments.LAchildID

Header = CINTable.Header
ReferenceDate = Header.ReferenceDate


# define characteristics of rule
@rule_definition(
    code="8736",
    module=CINTable.Assessments,
    message="For an Assessment that has not been completed, the start date must fall within the census year",
    affected_fields=[AssessmentAuthorisationDate, AssessmentActualStartDate],
)
def validate(
    data_container: Mapping[CINTable, pd.DataFrame], rule_context: RuleContext
):
    # PREPARING DATA

    df = data_container[Assessments]
    header = data_container[Header]
    ref_date_series = header[ReferenceDate]
    collection_start, collection_end = make_census_period(ref_date_series)

    # Before you begin, rename the index so that the initial row positions can be kept intact.
    df.index.name = "ROW_ID"

    # lOGIC
    # Implement rule logic as described by the Github issue.
    # Put the description as a comment above the implementation as shown.

    # Where present, if an <Assessments> group does not contain the <AssessmentAuthorisationDate> (N00160) then the <AssessmentActualStartDate> (N00159) must be on or between [Start_Of_Census_Year] and <ReferenceDate> (N00603)
    condition_1 = (df[CINdetailsID].notna()) & (df[AssessmentAuthorisationDate].isna())
    condition_2 = (collection_start <= df[AssessmentActualStartDate]) & (
        df[AssessmentActualStartDate] <= collection_end
    )

    # get all the data that fits the failing condition. Reset the index so that ROW_ID now becomes a column of df
    df_issues = df[condition_1 & ~condition_2].reset_index()

    # SUBMIT ERRORS
    # Generate a unique ID for each instance of an error.
    link_id = tuple(
        zip(
            df_issues[LAchildID],
            df_issues[CINdetailsID],
            df_issues[AssessmentActualStartDate],
        )
    )
    df_issues["ERROR_ID"] = link_id
    df_issues = (
        df_issues.groupby("ERROR_ID", group_keys=False)["ROW_ID"]
        .apply(list)
        .reset_index()
    )
    rule_context.push_type_1(
        table=Assessments,
        columns=[AssessmentAuthorisationDate, AssessmentActualStartDate],
        row_df=df_issues,
    )


def test_validate():
    # Create some sample data such that some values pass the validation and some fail.

    fake_header = pd.DataFrame(
        [{ReferenceDate: "31/03/2022"}]  # the census start date here will be 01/04/2021
    )

    child_assessments = pd.DataFrame(
        [
            {
                "CINdetailsID": pd.NA,
                "LAchildID": "child1",
                "AssessmentAuthorisationDate": pd.NA,
                "AssessmentActualStartDate": "27/05/2021",
            },
            {
                "CINdetailsID": "1",
                "LAchildID": "child2",
                "AssessmentAuthorisationDate": pd.NA,
                "AssessmentActualStartDate": "26/03/2020",
                # 1 error: start date is outside the reporting period
            },
            {
                "CINdetailsID": "1",
                "LAchildID": "child3",
                "AssessmentAuthorisationDate": "27/06/2021",
                "AssessmentActualStartDate": "27/05/2021",
            },
            {
                "CINdetailsID": "1",
                "LAchildID": "child4",
                "AssessmentAuthorisationDate": pd.NA,
                "AssessmentActualStartDate": "10/04/2024",
                # 3 error: start date is outside the reporting period
            },
            {
                "CINdetailsID": "1",
                "LAchildID": "child5",
                "AssessmentAuthorisationDate": pd.NA,
                "AssessmentActualStartDate": "27/05/2022",
                # 4 error: start date is outside the reporting period
            },
            {
                "CINdetailsID": pd.NA,
                "LAchildID": "child6",
                "AssessmentAuthorisationDate": pd.NA,
                "AssessmentActualStartDate": pd.NA,
            },
        ]
    )
    # if rule requires columns containing date values, convert those columns to datetime objects first. Do it here in the test_validate function, not above.
    child_assessments[AssessmentAuthorisationDate] = pd.to_datetime(
        child_assessments[AssessmentAuthorisationDate],
        format="%d/%m/%Y",
        errors="coerce",
    )
    child_assessments[AssessmentActualStartDate] = pd.to_datetime(
        child_assessments[AssessmentActualStartDate], format="%d/%m/%Y", errors="coerce"
    )

    # Run rule function passing in our sample data
    result = run_rule(validate, {Assessments: child_assessments, Header: fake_header})

    # Use .type1_issues to check for the result of .push_type1_issues() which you used above.
    issues = result.type1_issues

    issue_table = issues.table
    assert issue_table == Assessments

    # check that the right columns were returned. Replace CPPstartDate and CPPendDate with a list of your columns.
    issue_columns = issues.columns
    assert issue_columns == [AssessmentAuthorisationDate, AssessmentActualStartDate]

    # check that the location linking dataframe was formed properly.
    issue_rows = issues.row_df
    # replace 3 with the number of failing points you expect from the sample data.
    assert len(issue_rows) == 3
    # check that the failing locations are contained in a DataFrame having the appropriate columns. These lines do not change.
    assert isinstance(issue_rows, pd.DataFrame)
    assert issue_rows.columns.to_list() == ["ERROR_ID", "ROW_ID"]

    # Create the dataframe which you expect, based on the fake data you created. It should have two columns.
    # - The first column is ERROR_ID which contains the unique combination that identifies each error instance, which you decided on earlier.
    # - The second column in ROW_ID which contains a list of index positions that belong to each error instance.

    # The ROW ID values represent the index positions where you expect the sample data to fail the validation check.
    expected_df = pd.DataFrame(
        [
            {
                "ERROR_ID": (
                    "child2",
                    "1",
                    pd.to_datetime("26/03/2020", format="%d/%m/%Y", errors="coerce"),
                ),
                "ROW_ID": [1],
            },
            {
                "ERROR_ID": (
                    "child4",
                    "1",
                    pd.to_datetime("10/04/2024", format="%d/%m/%Y", errors="coerce"),
                ),
                "ROW_ID": [3],
            },
            {
                "ERROR_ID": (
                    "child5",
                    "1",
                    pd.to_datetime("27/05/2022", format="%d/%m/%Y", errors="coerce"),
                ),
                "ROW_ID": [4],
            },
        ]
    )
    assert issue_rows.equals(expected_df)

    # Check that the rule definition is what you wrote in the context above.

    # replace '8736' with the rule code and put the appropriate message in its place too.
    assert result.definition.code == "8736"
    assert (
        result.definition.message
        == "For an Assessment that has not been completed, the start date must fall within the census year"
    )
