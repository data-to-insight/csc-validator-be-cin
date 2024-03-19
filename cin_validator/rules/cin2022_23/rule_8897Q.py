from typing import Mapping

import pandas as pd

from cin_validator.rule_engine import CINTable, RuleContext, RuleType, rule_definition
from cin_validator.test_engine import run_rule
from cin_validator.utils import make_census_period

# Get tables and columns of interest from the CINTable object defined in rule_engine/__api.py

Assessments = CINTable.Assessments
AssessmentAuthorisationDate = Assessments.AssessmentAuthorisationDate
AssessmentFactors = Assessments.AssessmentFactors
LAchildID = Assessments.LAchildID
AssessmentID = Assessments.AssessmentID

AssessmentFactorsList = CINTable.AssessmentFactorsList
AssessmentFactor = AssessmentFactorsList.AssessmentFactor

Header = CINTable.Header
ReferenceDate = Header.ReferenceDate


# define characteristics of rule
@rule_definition(
    code="8897Q",
    module=CINTable.Assessments,
    rule_type=RuleType.QUERY,
    # replace the message with the corresponding value for this rule, gotten from the excel sheet.
    message="Parental or child factors at assessment information is missing from a completed assessment",
    # The column names tend to be the words within the < > signs in the github issue description.
    affected_fields=[AssessmentAuthorisationDate, AssessmentFactors],
)
def validate(
    data_container: Mapping[CINTable, pd.DataFrame], rule_context: RuleContext
):
    # PREPARING DATA

    df_ass = data_container[Assessments]
    df_asslist = data_container[AssessmentFactorsList]

    header = data_container[Header]
    ref_date_series = header[ReferenceDate]
    collection_start, collection_end = make_census_period(ref_date_series)

    # Before you begin, rename the index so that the initial row positions can be kept intact.
    df_ass.index.name = "ROW_ID"
    df_ass.reset_index(inplace=True)

    # lOGIC
    # Implement rule logic as described by the Github issue.
    # Put the description as a comment above the implementation as shown.

    # Where present, if <AssessmentAuthorisationDate> (N00160) is on or after [Start_Of_Census_Year] then one or more <AssessmentFactors>
    # (N00181) must be present within the same assessment module and must be a valid code
    # Get collection period
    factors_list = [
        "1A",
        "1B",
        "1C",
        "2A",
        "2B",
        "2C",
        "3A",
        "3B",
        "3C",
        "4A",
        "4B",
        "4C",
        "5A",
        "5B",
        "5C",
        "6A",
        "6B",
        "6C",
        "7A",
        "8B",
        "8C",
        "8D",
        "8E",
        "8F",
        "9A",
        "10A",
        "11A",
        "12A",
        "13A",
        "14A",
        "15A",
        "16A",
        "17A",
        "18A",
        "18B",
        "18C",
        "19A",
        "19B",
        "19C",
        "20",
        "21",
        "22A",
        "23A",
        "24A",
    ]

    df_ass_merged = df_ass.merge(
        df_asslist[["LAchildID", "AssessmentID", "AssessmentFactor"]],
        on=["LAchildID", "AssessmentID"],
        how="left",
    )

    condition1 = (df_ass_merged[AssessmentAuthorisationDate] >= collection_start) & (
        df_ass_merged[AssessmentAuthorisationDate].notna()
    )
    condition2 = (df_ass_merged[AssessmentFactors].notna()) & (
        df_ass_merged[AssessmentFactor].isin(factors_list)
    )

    # get all the data that fits the failing condition. Reset the index so that ROW_ID now becomes a column of df
    df_issues = df_ass_merged[condition1 & ~condition2].reset_index()

    # SUBMIT ERRORS
    # Generate a unique ID for each instance of an error. In this case,
    # - If only LAchildID is used as an identifier, multiple instances of the error on a child will be understood as 1 instance.
    # We don't want that because in reality, a child can have multiple instances of an error.
    # - If we use the LAchildID-CPPstartDate combination, that artificially cancels out the instances where a start date repeats for the same child.
    # Another rule checks for that condition. Not this one.
    # - It is very unlikely that a combination of LAchildID-CPPstartDate-CPPendDate will repeat in the DataFrame.
    # Hence, it can be used as a unique identifier of the row.

    # Replace CPPstartDate and CPPendDate below with the columns concerned in your rule.
    link_id = tuple(
        zip(
            df_issues[LAchildID],
            df_issues[AssessmentID],
            df_issues[AssessmentFactors],
        )
    )
    df_issues["ERROR_ID"] = link_id
    df_issues = (
        df_issues.groupby("ERROR_ID", group_keys=False)["ROW_ID"]
        .apply(list)
        .reset_index()
    )
    # Ensure that you do not change the ROW_ID, and ERROR_ID column names which are shown above. They are keywords in this project.
    rule_context.push_type_1(
        table=Assessments,
        columns=[AssessmentAuthorisationDate, AssessmentFactors],
        row_df=df_issues,
    )


def test_validate():
    # Create some sample data such that some values pass the validation and some fail.
    fake_data = pd.DataFrame(
        [
            {
                "LAchildID": "child1",
                "AssessmentID": "11",
                "AssessmentFactors": pd.NA,
                "AssessmentAuthorisationDate": "26/05/2000",
            },  # Fails as no assessment factor code
            {
                "LAchildID": "child2",
                "AssessmentID": "21",
                "AssessmentFactors": "99",
                "AssessmentAuthorisationDate": "26/05/2000",
            },  # Fails as incorrect assessment factor code
            {
                "LAchildID": "child3",
                "AssessmentID": "31",
                "AssessmentFactors": "1A",
                "AssessmentAuthorisationDate": "26/05/2000",
            },
            {
                "LAchildID": "child3",
                "AssessmentID": "32",
                "AssessmentAuthorisationDate": "26/05/2000",
                "AssessmentFactors": pd.NA,
            },  # Fails as no factor selected
            {
                "LAchildID": "child4",
                "AssessmentID": "41",
                "AssessmentFactors": "1A",
                "AssessmentAuthorisationDate": "26/05/2000",
            },
            {
                "LAchildID": "child5",
                "AssessmentID": "51",
                "AssessmentAuthorisationDate": pd.NA,
                "AssessmentFactors": pd.NA,
            },
            {
                "LAchildID": "child5",
                "AssessmentID": "52",
                "AssessmentAuthorisationDate": "26/05/1945",
                "AssessmentFactors": pd.NA,  # Passes as before census year
            },
        ]
    )
    # if rule requires columns containing date values, convert those columns to datetime objects first. Do it here in the test_validate function, not above.

    fake_data[AssessmentAuthorisationDate] = pd.to_datetime(
        fake_data[AssessmentAuthorisationDate], format="%d/%m/%Y", errors="coerce"
    )

    sample_header = pd.DataFrame([{ReferenceDate: "31/03/2001"}])

    # Run rule function passing in our sample data
    result = run_rule(
        validate,
        {
            Assessments: fake_data,
            AssessmentFactorsList: fake_data.rename(
                columns={"AssessmentFactors": "AssessmentFactor"}
            ),
            Header: sample_header,
        },
    )

    # Use .type1_issues to check for the result of .push_type1_issues() which you used above.
    issues = result.type1_issues

    issue_table = issues.table
    assert issue_table == Assessments

    issue_columns = issues.columns
    assert issue_columns == [AssessmentAuthorisationDate, AssessmentFactors]

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
                    "child1",
                    "11",
                    pd.NA,
                ),
                "ROW_ID": [0],
            },
            {
                "ERROR_ID": (
                    "child2",
                    "21",
                    "99",
                ),
                "ROW_ID": [1],
            },
            {
                "ERROR_ID": (
                    "child3",
                    "32",
                    pd.NA,
                ),
                "ROW_ID": [3],
            },
        ]
    )
    assert issue_rows.equals(expected_df)

    # Check that the rule definition is what you wrote in the context above.

    # replace 8897Q with the rule code and put the appropriate message in its place too.
    assert result.definition.code == "8897Q"
    assert (
        result.definition.message
        == "Parental or child factors at assessment information is missing from a completed assessment"
    )
