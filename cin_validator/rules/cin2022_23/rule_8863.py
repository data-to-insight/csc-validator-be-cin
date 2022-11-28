from typing import Mapping

import pandas as pd

from cin_validator.rule_engine import CINTable, RuleContext, rule_definition
from cin_validator.rules.cin2022_23.rule_8925 import LAchildID
from cin_validator.test_engine import run_rule

# Get tables and columns of interest from the CINTable object defined in rule_engine/__api.py

Assessments = CINTable.Assessments
AssessmentActualStartDate = Assessments.AssessmentActualStartDate
AssessmentAuthorisationDate = Assessments.AssessmentAuthorisationDate
LAchildID = Assessments.LAchildID
CINdetailsID = Assessments.CINdetailsID

# define characteristics of rule
@rule_definition(
    code=8863,
    module=CINTable.Assessments,
    message="An Assessment is shown as starting when there is another Assessment ongoing",
    affected_fields=[AssessmentActualStartDate, AssessmentAuthorisationDate],
)
def validate(
    data_container: Mapping[CINTable, pd.DataFrame], rule_context: RuleContext
):
    # PREPARING DATA

    # Name of the table required.
    df = data_container[Assessments]
    # Before you begin, rename the index and make it a column, so that the initial row positions can be kept intact.
    df.index.name = "ROW_ID"
    df.reset_index(inplace=True)

    # lOGIC
    # Implement rule logic as described by the Github issue.
    # Put the description as a comment above the implementation as shown.

    # Within one <CINdetails> group, where present each <AssessmentActualStartDate> (N00159) must not fall on or between:
    # a) the <AssessmentActualStartDate> (N00159) and <AssessmentAuthorisation Date (N00160) of any other <Assessments> group that has an <AssessmentAuthorisationDate> (N00160)
    # OR
    # b) the <AssessmentActualStartDate> (N00159) and the <ReferenceDate> (N00603) where the <AssessmentAuthorisationDate> (N00160) is missing

    # DF_CHECK: APPLY GROUPBYs IN A SEPARATE DATAFRAME SO THAT OTHER COLUMNS ARE NOT LOST OR CORRUPTED. THEN, MAP THE RESULTS TO THE INITIAL DATAFRAME.
    df_check = df.copy()

    # Get all the locations where an AssessmentActualStartDate is not null and also where the AssessmentAuthorisation is null
    df_check = df_check[df_check[AssessmentActualStartDate].notna()]
    df_check = df_check[df_check[AssessmentAuthorisationDate].isna()]

    # We'll have to count the number of nan values per group. NaNs cannot be counted so replace them with something that can.
    # Do this only if your rule requires that you interact with a column made up of all NaNs.
    df_check[AssessmentActualStartDate].fillna(1, inplace=True)
    df_check[AssessmentAuthorisationDate].fillna(1, inplace=True)

    # count how many occurences of AssessmentActualStartDate per CINdetails group in each child.
    df_check = (
        df_check.groupby([LAchildID, CINdetailsID])[AssessmentActualStartDate]
        .count()
        .reset_index()
    )
    #
    # when you groupby as shown above a series is returned where the columns in the round brackets become the index and the groupby result are the values.
    # resetting the index pushes the columns in the () back as columns of the dataframe and assigns the groupby result to the column in the square bracket.
    #
    # Filter out those instances where AssessmentActualStartDate has been entered more than once in a CINdetails group.
    df_check = df_check[df_check[AssessmentActualStartDate] > 1]

    issue_ids = tuple(zip(df_check[LAchildID], df_check[CINdetailsID]))

    # DF_ISSUES: GET ALL THE DATA ABOUT THE LOCATIONS THAT WERE IDENTIFIED IN DF_CHECK
    df["ERROR_ID"] = tuple(zip(df[LAchildID], df[CINdetailsID]))
    df_issues = df[df.ERROR_ID.isin(issue_ids)]

    df_issues = (
        df_issues.groupby("ERROR_ID", group_keys=False)["ROW_ID"]
        .apply(list)
        .reset_index()
    )
    # Ensure that you do not change the ROW_ID, and ERROR_ID column names which are shown above. They are keywords in this project.
    rule_context.push_type_3(
        table=Assessments,
        columns=[AssessmentActualStartDate, AssessmentAuthorisationDate],
        row_df=df_issues,
    )


def test_validate():
    # Create some sample data such that some values pass the validation and some fail.
    sample_assessments = pd.DataFrame(
        [  # child1
            {  # fail - Assessment has started on the child with no Authorisation Date on the previous Assessment
                LAchildID: "child1",
                CINdetailsID: "cinID1",
                AssessmentActualStartDate: "10/04/2021",
                AssessmentAuthorisationDate: pd.NA,
            },
            {  # fail - A further Assessment has started on the child with no Authorisation Date for the first Assessment
                LAchildID: "child1",
                CINdetailsID: "cinID1",
                AssessmentActualStartDate: "01/04/2021",
                AssessmentAuthorisationDate: pd.NA,
            },
            # child2
            {  # pass - Previous Assessment has been authorised
                LAchildID: "child2",
                CINdetailsID: "cinID2",
                AssessmentActualStartDate: "10/10/2021",
                AssessmentAuthorisationDate: pd.NA,
            },
            {  # pass - Assessment has been authorised.
                LAchildID: "child2",
                CINdetailsID: "cinID2",
                AssessmentActualStartDate: "10/06/2021",
                AssessmentAuthorisationDate: "31/08/2021",
            },
            # child3
            {  # fail - Assessment has not been authorised
                LAchildID: "child3",
                CINdetailsID: "cinID1",
                AssessmentActualStartDate: "10/12/2021",
                AssessmentAuthorisationDate: pd.NA,
            },
            {  # fail - - Assessment has not been authorised
                LAchildID: "child3",
                CINdetailsID: "cinID1",
                AssessmentActualStartDate: "31/03/2022",
                AssessmentAuthorisationDate: pd.NA,
            },
            # child4
            {  # pass - Assessment authorised
                LAchildID: "child4",
                CINdetailsID: "cinID1",
                AssessmentActualStartDate: "01/03/2022",
                AssessmentAuthorisationDate: "03/03/2002",
            },
            {  # pass - only 1 Assessment ongoing. Previous assessment has been authorised.
                LAchildID: "child4",
                CINdetailsID: "cinID1",
                AssessmentActualStartDate: "10/03/2022",
                AssessmentAuthorisationDate: pd.NA,
            },
        ]
    )

    # if rule requires columns containing date values, convert those columns to datetime objects first. Do it here in the test_validate function, not above.
    sample_assessments[AssessmentActualStartDate] = pd.to_datetime(
        sample_assessments[AssessmentActualStartDate],
        format="%d/%m/%Y",
        errors="coerce",
    )
    sample_assessments[AssessmentAuthorisationDate] = pd.to_datetime(
        sample_assessments[AssessmentAuthorisationDate],
        format="%d/%m/%Y",
        errors="coerce",
    )

    # Run rule function passing in our sample data
    result = run_rule(validate, {Assessments: sample_assessments})

    # Use .type3_issues to check for the result of .push_type3_issues() which you used above.
    issues_list = result.type3_issues
    # Issues list contains the objects pushed in their respective order. Since push_type3 was only used once, there will be one object in issues_list.
    assert len(issues_list) == 1

    issues = issues_list[0]

    # get table name and check it.
    issue_table = issues.table
    assert issue_table == Assessments

    # check that the right columns were returned.
    issue_columns = issues.columns
    assert issue_columns == [AssessmentActualStartDate, AssessmentAuthorisationDate]

    # check that the location linking dataframe was formed properly.
    issue_rows = issues.row_df
    # replace 2 with the number of failing points you expect from the sample data.
    assert len(issue_rows) == 2
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
                    "cinID1",
                ),
                "ROW_ID": [0, 1],
            },
            {
                "ERROR_ID": (
                    "child3",
                    "cinID1",
                ),
                "ROW_ID": [4, 5],
            },
        ]
    )
    assert issue_rows.equals(expected_df)

    # Check that the rule definition is what you wrote in the context above.

    # replace 8925 with the rule code and put the appropriate message in its place too.
    assert result.definition.code == 8863
    assert (
        result.definition.message
        == "An Assessment is shown as starting when there is another Assessment ongoing"
    )
