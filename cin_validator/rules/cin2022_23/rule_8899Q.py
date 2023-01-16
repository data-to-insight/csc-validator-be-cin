from typing import Mapping

import pandas as pd

from cin_validator.rule_engine import CINTable, RuleContext, RuleType, rule_definition
from cin_validator.test_engine import run_rule
from cin_validator.utils import make_census_period

# Get tables and columns of interest from the CINTable object defined in rule_engine/__api.py

Disabilities = CINTable.Disabilities
Disability = Disabilities.Disability
LAchildID = Disabilities.LAchildID

Assessments = CINTable.Assessments
AssessmentAuthorisationDate = Assessments.AssessmentAuthorisationDate
AssessmentFactors = Assessments.AssessmentFactors


# Reference date in header is needed to define the period of census.
Header = CINTable.Header
ReferenceDate = Header.ReferenceDate

# define characteristics of rule
@rule_definition(
    # write the rule code here, in place of 8899Q
    code="8899Q",
    rule_type=RuleType.QUERY,
    # replace Assessments with the value in the module column of the excel sheet corresponding to this rule .
    # Note that even if multiple tables are involved, one table will be named in the module column.
    module=CINTable.Assessments,
    # replace the message with the corresponding value for this rule, gotten from the excel sheet.
    message="Please check: A child identified as having a disability does not have a disability factor recorded at the end of assessment.",
    # The column names tend to be the words within the < > signs in the github issue description.
    affected_fields=[
        AssessmentFactors,
    ],
)
def validate(
    data_container: Mapping[CINTable, pd.DataFrame], rule_context: RuleContext
):
    # PREPARING DATA

    df_ass = data_container[Assessments].copy()
    df_dis = data_container[Disabilities].copy()

    # Before you begin, rename the index so that the initial row positions can be kept intact.
    df_ass.index.name = "ROW_ID"
    df_dis.index.name = "ROW_ID"

    df_ass.reset_index(inplace=True)
    df_dis.reset_index(inplace=True)

    # get collection period
    header = data_container[Header]
    ref_date_series = header[ReferenceDate]
    collection_start, collection_end = make_census_period(ref_date_series)

    # If disability is not NONE and <AssessmentAuthorisationDate> (N00160) is on or after [Start_Of_Census_Year] then <AssessmentFactors> (must include one or both of 5A or 6A)
    #  Returns rows without NONE or NANs (to catch exceptions in formatting).
    df_dis = df_dis[(df_dis[Disability].str.upper() != "NONE")]

    # Returns only rows that happened after the census start
    df_ass = df_ass[df_ass[AssessmentAuthorisationDate] >= collection_start]

    merged_df = df_dis.copy().merge(
        df_ass.copy(),
        on=["LAchildID"],
        suffixes=["_dis", "_ass"],
    )
    # filter out asssessment groups that have an AssessmentFactor of 5A or 6A across the records of the group.
    # all assessments that have the same AssessmentAuthorisationDate are considered as belonging to one group.
    good = merged_df[merged_df[AssessmentFactors].isin(["5A", "6A"])]
    good_ids = tuple(
        zip(good[LAchildID], good[AssessmentAuthorisationDate])
    )  # ids of groups that should pass
    merged_df["group_ids"] = tuple(
        zip(merged_df[LAchildID], merged_df[AssessmentAuthorisationDate])
    )
    merged_df = merged_df[
        ~merged_df.group_ids.isin(good_ids)
    ]  # filter out such that only failing rows are left

    # create an identifier for each error instance.
    merged_df["ERROR_ID"] = tuple(
        zip(merged_df[LAchildID], merged_df[AssessmentAuthorisationDate])
    )

    # we can now map the suffixes columns to their corresponding source tables such that the failing ROW_IDs and ERROR_IDs exist per table.
    df_ass_issues = (
        df_ass.merge(merged_df, left_on="ROW_ID", right_on="ROW_ID_ass")
        .groupby("ERROR_ID", group_keys=False)["ROW_ID"]
        .apply(list)
        .reset_index()
    )
    df_dis_issues = (
        df_dis.merge(merged_df, left_on="ROW_ID", right_on="ROW_ID_dis")
        .groupby("ERROR_ID", group_keys=False)["ROW_ID"]
        .apply(list)
        .reset_index()
    )

    # Ensure that you maintain the ROW_ID, and ERROR_ID column names which are shown above. They are keywords in this project.
    rule_context.push_type_2(
        table=Assessments, columns=[AssessmentFactors], row_df=df_ass_issues
    )
    rule_context.push_type_2(
        table=Disabilities, columns=[Disability], row_df=df_dis_issues
    )


def test_validate():
    # Create some sample data such that some values pass the validation and some fail.
    sample_ass = pd.DataFrame(
        [
            {
                "LAchildID": "child1",
                "AssessmentAuthorisationDate": "31/03/2001",  # 0 pass
                "AssessmentFactors": "Nil",
            },
            {
                "LAchildID": "child1",
                "AssessmentAuthorisationDate": pd.NA,  # 1 ignored
                "AssessmentFactors": "Nil",
            },
            {
                "LAchildID": "child2",
                "AssessmentAuthorisationDate": "23/03/2001",  # 2 pass
                "AssessmentFactors": "Nil",
            },
            {
                "LAchildID": "child10",
                "AssessmentAuthorisationDate": "31/03/2001",  # 3 fail ass factor not allowed
                "AssessmentFactors": "Nil",
            },
            {
                "LAchildID": "child3",
                "AssessmentAuthorisationDate": "31/03/2001",  # 4 ignore as other factor in group is 5A
                "AssessmentFactors": "Nil",
            },
            {  # fail
                "LAchildID": "child9",
                "AssessmentAuthorisationDate": "31/03/2001",  # 5 fail ass factor not allowed
                "AssessmentFactors": "Nil",
            },
            {
                "LAchildID": "child3",
                "AssessmentAuthorisationDate": "31/03/2001",  # 6 pass
                "AssessmentFactors": "5A",
            },
        ]
    )
    sample_dis = pd.DataFrame(
        [
            {  # 0 ignore Disability=="NONE"
                "LAchildID": "child1",
                "Disability": "NONE",
            },
            {  # 1 ignore Disability=="NONE"
                "LAchildID": "child1",
                "Disability": "NONE",
            },
            {  # 2 ignore Disability=="NONE. would have failed"
                "LAchildID": "child2",
                "Disability": "None",  # will be evaluated as "NONE".
            },
            {  # 3 fail
                "LAchildID": "child10",
                "Disability": "Blind",
            },
            {  # 4 ignore Disability=="NONE"
                "LAchildID": "child3",
                "Disability": "NONE",
            },
            {  # 5 fail
                "LAchildID": "child3",
                "Disability": "Blind",
            },
            {  # 6 ignore Disability=="NONE"
                "LAchildID": "child9",
                "Disability": "NONE",
            },
        ]
    )

    # if rule requires columns containing date values, convert those columns to datetime objects first. Do it here in the test_validate function, not above.
    sample_ass[AssessmentAuthorisationDate] = pd.to_datetime(
        sample_ass[AssessmentAuthorisationDate], format="%d/%m/%Y", errors="coerce"
    )
    sample_header = pd.DataFrame(
        [{ReferenceDate: "31/03/2001"}]  # the census start date here will be 01/04/2000
    )

    # Run the rule function, passing in our sample data.
    result = run_rule(
        validate,
        {
            Assessments: sample_ass,
            Disabilities: sample_dis,
            Header: sample_header,
        },
    )

    # Use .type2_issues to check for the result of .push_type2_issues() which you used above.
    issues_list = result.type2_issues
    assert len(issues_list) == 2
    # the function returns a list on NamedTuples where each NamedTuple contains (table, column_list, df_issues)
    # pick any table and check it's values. the tuple in location 0 will contain the Assessments columns because that's the first thing pushed above.
    issues = issues_list[0]

    # get table name and check it. Replace Assessments with the name of your table.
    issue_table = issues.table
    assert issue_table == Assessments

    # check that the right columns were returned. Replace AssessmentFactors  with a list of your columns.
    issue_columns = issues.columns
    assert issue_columns == [AssessmentFactors]

    # check that the location linking dataframe was formed properly.
    issue_rows = issues.row_df
    # replace 2 with the number of failing points you expect from the sample data.
    assert len(issue_rows) == 1
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
                    "child10",
                    pd.to_datetime("31/03/2001", format="%d/%m/%Y", errors="coerce"),
                ),
                "ROW_ID": [3],
            },
        ]
    )
    assert issue_rows.equals(expected_df)

    # Check that the rule definition is what you wrote in the context above.

    # replace 8899Q with the rule code and put the appropriate message in its place too.
    assert result.definition.code == "8899Q"
    assert (
        result.definition.message
        == "Please check: A child identified as having a disability does not have a disability factor recorded at the end of assessment."
    )
