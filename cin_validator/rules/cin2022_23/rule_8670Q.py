"""
Rule number: 8670Q
Module: Assessments
Rule details: Where present, within an <Assessments> group, if <AssessmentAuthorisationDate> (N00160) is not present then <AssessmentActualStartDate>
(N00159) should not be before the <ReferenceDate> (N00603) minus 45 working days.

Rule message: Please check: Assessment started more than 45 working days before the end of the census year. However, there is no Assessment end date. 

"""
from typing import Mapping

import pandas as pd
import datetime as dt

from cin_validator.rule_engine import (
    CINTable,
    IssueLocator,
    RuleContext,
    RuleType,
    rule_definition,
)
from cin_validator.test_engine import run_rule
from cin_validator.utils import make_census_period

# Get tables and columns of interest from the CINTable object defined in rule_engine/__api.py

Assessments = CINTable.Assessments
AssessmentAuthorisationDate = Assessments.AssessmentAuthorisationDate
AssessmentActualStartDate = Assessments.AssessmentActualStartDate
LAchildID = Assessments.LAchildID

Header = CINTable.Header
ReferenceDate = Header.ReferenceDate

# define characteristics of rule
@rule_definition(
    code="8670Q",
    module=CINTable.Assessments,
    rule_type=RuleType.QUERY,
    message="Please check: Assessment started more than 45 working days before the end of the census year. However, there is no Assessment end date. ",
    affected_fields=[AssessmentActualStartDate],
)
def validate(
    data_container: Mapping[CINTable, pd.DataFrame], rule_context: RuleContext
):
    df_assessments = data_container[Assessments]
    df_assessments.index.name = "ROW_ID"

    df_ref = data_container[Header]
    ref_date_series = df_ref[ReferenceDate]
    collection_start, collection_end = make_census_period(ref_date_series)

    #  If <AssessmentAuthorisationDate> (N00160) is not present then <AssessmentActualStartDate> (N00159) should not be before the <ReferenceDate> (N00603) minus 45 working days

    # Filter to only those with no authorisation date
    df_assessments = df_assessments[df_assessments[AssessmentAuthorisationDate].isna()]

    # Find the reference date - 45
    latest_date = collection_end - pd.tseries.offsets.BDay(45)

    df_issues = df_assessments[
        df_assessments[AssessmentActualStartDate] < latest_date
    ].reset_index()

    link_id = tuple(zip(df_issues[LAchildID], df_issues[AssessmentActualStartDate]))
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
    sample_assessments = pd.DataFrame(
        [
            {
                "LAchildID": "ID1",
                "AssessmentAuthorisationDate": "26/05/2022",
                "AssessmentActualStartDate": "15/10/2022",
                # Pass, authorisation date present
            },
            {
                "LAchildID": "ID2",
                "AssessmentAuthorisationDate": pd.NA,
                "AssessmentActualStartDate": "15/10/2022",
                # Fail, start date is before ref date - 45wd
            },
            {
                "LAchildID": "ID3",
                "AssessmentAuthorisationDate": pd.NA,
                "AssessmentActualStartDate": "15/03/2023",
                # Pass, start date is withing 45wd of reference date
            },
        ]
    )

    sample_assessments[AssessmentAuthorisationDate] = pd.to_datetime(
        sample_assessments[AssessmentAuthorisationDate],
        format="%d/%m/%Y",
        errors="coerce",
    )
    sample_assessments[AssessmentActualStartDate] = pd.to_datetime(
        sample_assessments[AssessmentActualStartDate],
        format="%d/%m/%Y",
        errors="coerce",
    )

    sample_header = pd.DataFrame([{"ReferenceDate": "31/03/2023"}])

    sample_header[ReferenceDate] = pd.to_datetime(
        sample_header[ReferenceDate], format="%d/%m/%Y", errors="coerce"
    )

    # Run rule function passing in our sample data
    result = run_rule(
        validate, {Assessments: sample_assessments, Header: sample_header}
    )

    # Use .type1_issues to check for the result of .push_type1_issues() which you used above.
    issues = result.type1_issues

    # get table name and check it. Replace ChildIdentifiers with the name of your table.
    issue_table = issues.table
    assert issue_table == Assessments

    # check that the right columns were returned. Replace PersonDeathDate and PersonBirthDate with a list of your columns.
    issue_columns = issues.columns
    assert issue_columns == [AssessmentAuthorisationDate, AssessmentActualStartDate]

    # check that the location linking dataframe was formed properly.
    issue_rows = issues.row_df
    # replace 2 with the number of failing points you expect from the sample data.
    assert len(issue_rows) == 1
    # check that the failing locations are contained in a DataFrame having the appropriate columns. These lines do not change.
    assert isinstance(issue_rows, pd.DataFrame)
    assert issue_rows.columns.to_list() == ["ERROR_ID", "ROW_ID"]

    # Create the dataframe which you expect, based on the fake data you created. It should have two columns.
    expected_df = pd.DataFrame(
        [
            {
                "ERROR_ID": (
                    "ID2",
                    pd.to_datetime("15/10/2022", format="%d/%m/%Y", errors="coerce"),
                ),
                "ROW_ID": [1],
            },
        ]
    )

    assert issue_rows.equals(expected_df)

    # replace 8500 with the rule code and put the appropriate message in its place too.
    assert result.definition.code == "8670Q"
    assert (
        result.definition.message
        == "Please check: Assessment started more than 45 working days before the end of the census year. However, there is no Assessment end date. "
    )
