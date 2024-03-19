from typing import Mapping

import pandas as pd

from cin_validator.rule_engine import CINTable, RuleContext, rule_definition
from cin_validator.test_engine import run_rule

# Get tables and columns of interest from the CINTable object defined in rule_engine/__api.py

Assessments = CINTable.Assessments
AssessmentID = Assessments.AssessmentID
AssessmentFactors = Assessments.AssessmentFactors
AssessmentActualStartDate = Assessments.AssessmentActualStartDate
LAchildID = Assessments.LAchildID
CINdetailsID = Assessments.CINdetailsID

AssessmentFactorsList = CINTable.AssessmentFactorsList
AssessmentFactor = AssessmentFactorsList.AssessmentFactor


# define characteristics of rule
@rule_definition(
    # write the rule code here, in place of '8898'
    code="8898",
    # replace Assessments with the value in the module column of the excel sheet corresponding to this rule .
    module=CINTable.Assessments,
    # replace the message with the corresponding value for this rule, gotten from the excel sheet.
    message=" The assessment has more than one parental or child factors with the same code",
    # The column names tend to be the words within the < > signs in the github issue description.
    affected_fields=[AssessmentFactors],
)
def validate(
    data_container: Mapping[CINTable, pd.DataFrame], rule_context: RuleContext
):
    df_ass = data_container[Assessments]
    df_asslist = data_container[AssessmentFactorsList]
    df_ass.index.name = "ROW_ID"
    df_ass.reset_index(inplace=True)

    df_ass_merged = df_ass.merge(
        df_asslist[["LAchildID", "CINdetailsID", "AssessmentID", "AssessmentFactor"]],
        on=["LAchildID", "CINdetailsID", "AssessmentID"],
    )

    # If there is more than one <AssessmentFactors> (N00181) for an assessment recorded, then none of the values should appear more than once for an assessment.

    # .duplicated() returns True in all the locations where LAchildID-CINdetailsID-AssessmentID-AssessmentFactors combination is a duplicate
    condition = df_ass_merged.duplicated(
        [LAchildID, CINdetailsID, AssessmentID, AssessmentFactor],
        keep=False,
    )

    df_issues = df_ass_merged[condition].drop_duplicates().reset_index()

    df_issues["ERROR_ID"] = tuple(
        zip(
            df_issues[LAchildID],
            df_issues[AssessmentID],
            df_issues[AssessmentFactors],
        )
    )

    df_issues = (
        df_issues.groupby("ERROR_ID", group_keys=False)["ROW_ID"]
        .apply(list)
        .reset_index()
    )

    # Ensure that you do not change the ROW_ID, and ERROR_ID column names which are shown above. They are keywords in this project.
    rule_context.push_type_3(
        table=Assessments, columns=[AssessmentFactors], row_df=df_issues
    )


def test_validate():
    # Create some sample data such that some values pass the validation and some fail.
    sample_assessments = pd.DataFrame(
        [  # child1
            {
                LAchildID: "child1",
                CINdetailsID: "cinID1",
                AssessmentActualStartDate: "01/01/2000",
                AssessmentFactors: ("111", "111"),  # fail: duplicate Assessment Factor
                AssessmentID: "1",  # fail: duplicate Assessment Factor
            },
            {
                LAchildID: "child1",
                CINdetailsID: "cinID1",
                AssessmentActualStartDate: "02/01/2000",
                AssessmentFactors: ("111", "222"),
                AssessmentID: "2",  # pass: different Assessment Factor
            },
        ]
    )
    sample_factors = pd.DataFrame(
        [  # child1
            {
                LAchildID: "child1",
                CINdetailsID: "cinID1",
                AssessmentFactor: "111",  # fail: duplicate Assessment Factor
                AssessmentID: "1",
            },
            {
                LAchildID: "child1",
                CINdetailsID: "cinID1",
                AssessmentFactor: "111",  # fail: duplicate Assessment Factor
                AssessmentID: "1",
            },
            {
                LAchildID: "child1",
                CINdetailsID: "cinID1",
                AssessmentFactor: "111",  # pass: different Assessment Factor
                AssessmentID: "2",
            },
            {
                LAchildID: "child1",
                CINdetailsID: "cinID2",
                AssessmentFactor: "222",  # pass: different Assessment Factor
                AssessmentID: "2",
            },
        ]
    )
    # if rule requires columns containing date values, convert those columns to datetime objects first. Do it here in the test_validate function, not above.
    sample_assessments[AssessmentActualStartDate] = pd.to_datetime(
        sample_assessments[AssessmentActualStartDate],
        format="%d/%m/%Y",
        errors="coerce",
    )

    # Run rule function passing in our sample data
    result = run_rule(
        validate,
        {
            Assessments: sample_assessments,
            AssessmentFactorsList: sample_factors,
        },
    )

    # Use .type3_issues to check for the result of .push_type3_issues() which you used above.
    issues_list = result.type3_issues
    # Issues list contains the objects pushed in their respective order. Since push_type3 was only used once, there will be one object in issues_list.
    assert len(issues_list) == 1

    issues = issues_list[0]

    issue_table = issues.table
    assert issue_table == Assessments

    issue_columns = issues.columns
    assert issue_columns == [AssessmentFactors]

    # check that the location linking dataframe was formed properly.
    issue_rows = issues.row_df
    # replace 1 with the number of failing points you expect from the sample data.
    assert len(issue_rows) == 1
    # check that the failing locations are contained in a DataFrame having the appropriate columns. These lines do not change.
    assert isinstance(issue_rows, pd.DataFrame)
    assert issue_rows.columns.to_list() == ["ERROR_ID", "ROW_ID"]
    print(issue_rows)
    # Create the dataframe which you expect, based on the fake data you created. It should have two columns.
    # - The first column is ERROR_ID which contains the unique combination that identifies each error instance, which you decided on earlier.
    # - The second column in ROW_ID which contains a list of index positions that belong to each error instance.

    # The ROW ID values represent the index positions where you expect the sample data to fail the validation check.
    expected_df = pd.DataFrame(
        [
            {
                "ERROR_ID": (
                    "child1",
                    "1",
                    ("111", "111"),
                ),
                "ROW_ID": [0],
            },
        ]
    )
    assert issue_rows.equals(expected_df)

    # Check that the rule definition is what you wrote in the context above.

    # replace '8898' with the rule code and put the appropriate message in its place too.
    assert result.definition.code == "8898"
    assert (
        result.definition.message
        == " The assessment has more than one parental or child factors with the same code"
    )
