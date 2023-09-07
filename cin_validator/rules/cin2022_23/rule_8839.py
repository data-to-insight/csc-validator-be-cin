from typing import Mapping

import pandas as pd

from cin_validator.rule_engine import CINTable, RuleContext, rule_definition
from cin_validator.test_engine import run_rule

# Get tables and columns of interest from the CINTable object defined in rule_engine/__api.py

Section47 = CINTable.Section47
LAchildID = Section47.LAchildID
CINdetailsID = Section47.CINdetailsID
DateOfInitialCPC = Section47.DateOfInitialCPC
ICPCnotRequired = Section47.ICPCnotRequired


# define characteristics of rule
@rule_definition(
    code="8839",
    module=CINTable.Section47,
    # replace the message with the corresponding value for this rule, gotten from the excel sheet.
    message="Within one CINDetails group there are 2 or more open S47 Assessments",
    # The column names tend to be the words within the < > signs in the github issue description.
    affected_fields=[DateOfInitialCPC],
)
def validate(
    data_container: Mapping[CINTable, pd.DataFrame], rule_context: RuleContext
):
    # PREPARING DATA

    # Replace Section47 with the name of the table you need.
    df = data_container[Section47]
    # Before you begin, rename the index and make it a column, so that the initial row positions can be kept intact.
    df.index.name = "ROW_ID"
    df.reset_index(inplace=True)

    # lOGIC
    # Implement rule logic as described by the Github issue.
    # Put the description as a comment above the implementation as shown.

    # Within one <CINdetails> group, there must be only one <Section47> group that has no <DateOfInitialCPC> (N00110) recorded
    # OR <Section47> group that has a missing <DateOfInitialCPC> (N00110) and the <ICPCnotRequired> (N00111) flag is not true

    # DF_CHECK: APPLY GROUPBYs IN A SEPARATE DATAFRAME SO THAT OTHER COLUMNS ARE NOT LOST OR CORRUPTED. THEN, MAP THE RESULTS TO THE INITIAL DATAFRAME.
    df_check = df.copy()
    # get all the locations where ICPCnotRequired is null or not true (1)
    # The rule originally asks for true, not 1, but an analyst coded it for 1, so their LA may use 1 and 0 instead, as such, it now checks for either.
    df_check = df_check[
        df_check[ICPCnotRequired].isna()
        | (~df_check[ICPCnotRequired].astype(str).isin(["true", "1"]))
    ]
    # get all the locations where DateOfInitialCPC is null
    df_check = df_check[df_check[DateOfInitialCPC].isna()]
    # We'll have to count the number of nan values per group. NaNs cannot be counted so replace them with something that can.
    # Do this only if your rule requires that you interact with a column made up of all NaNs.
    # Adding as a separate column because I need the original column in the groupby for df_issues later on.
    df_check["CountICPC"] = df_check[DateOfInitialCPC]
    df_check["CountICPC"].fillna(1, inplace=True)
    # count how many occurences of missing DateOfInitialCPC per CINdetails group in each child.
    df_check = (
        df_check.groupby([LAchildID, CINdetailsID, DateOfInitialCPC], dropna=False)[
            "CountICPC"
        ]
        .count()
        .reset_index()
    )

    # when you groupby as shown above a series is returned where the columns in the round brackets become the index and the groupby result are the values.
    # resetting the index pushes the columns in the () back as columns of the dataframe and assigns the groupby result to the column in the square bracket.

    # filter out the instances where DateOfInitialCPC is missing more than once in a CINdetails group.
    df_check = df_check[df_check["CountICPC"] > 1]
    issue_ids = tuple(
        zip(df_check[LAchildID], df_check[CINdetailsID], df_check[DateOfInitialCPC])
    )
    # DF_ISSUES: GET ALL THE DATA ABOUT THE LOCATIONS THAT WERE IDENTIFIED IN DF_CHECK
    df["ERROR_ID"] = tuple(zip(df[LAchildID], df[CINdetailsID], df[DateOfInitialCPC]))
    df_issues = df[df.ERROR_ID.isin(issue_ids)]
    df_issues = (
        df_issues.groupby("ERROR_ID", group_keys=False)["ROW_ID"]
        .apply(list)
        .reset_index()
    )

    # Ensure that you do not change the ROW_ID, and ERROR_ID column names which are shown above. They are keywords in this project.
    rule_context.push_type_3(
        table=Section47, columns=[DateOfInitialCPC], row_df=df_issues
    )


def test_validate():
    # Create some sample data such that some values pass the validation and some fail.
    sample_s47 = pd.DataFrame(
        [  # child1
            {  # fail
                LAchildID: "child1",
                CINdetailsID: "cinID1",
                DateOfInitialCPC: pd.NA,  # 0 first nan date in group
                ICPCnotRequired: 0,
            },
            {  # fail
                LAchildID: "child1",
                CINdetailsID: "cinID1",
                DateOfInitialCPC: pd.NA,  # 1 second nan date in group
                ICPCnotRequired: 0,
            },
            {  # pass
                LAchildID: "child1",
                CINdetailsID: "cinID2",
                DateOfInitialCPC: pd.NA,  # 2 not more than one nan authorisation date in group
                ICPCnotRequired: 0,
            },
            # child2
            {  # pass
                LAchildID: "child2",
                CINdetailsID: "cinID2",
                DateOfInitialCPC: "26/05/2021",  # 3 not nan
                ICPCnotRequired: 0,
            },
            {  # fail
                LAchildID: "child2",
                CINdetailsID: "cinID2",
                DateOfInitialCPC: pd.NA,  # 4 first nan date in group
                ICPCnotRequired: pd.NA,
            },
            {  # fail
                LAchildID: "child2",
                CINdetailsID: "cinID2",
                DateOfInitialCPC: pd.NA,  # 5 second nan date in group
                ICPCnotRequired: pd.NA,
            },
            # child 3
            {  # pass
                LAchildID: "child3",
                CINdetailsID: "cinID3",
                DateOfInitialCPC: pd.NA,  # 6 nan but also ICPC not required
                ICPCnotRequired: "true",
            },
            {  # pass
                LAchildID: "child3",
                CINdetailsID: "cinID3",
                DateOfInitialCPC: pd.NA,  # 7 not more than one nan authorisation date in group
                ICPCnotRequired: pd.NA,
            },
            {  # pass
                LAchildID: "child3",
                CINdetailsID: "cinID3",
                DateOfInitialCPC: pd.NA,  # 6 nan but also ICPC not required
                ICPCnotRequired: 1,
            },
            {  # pass, 1 as string for issue 373
                LAchildID: "child3",
                CINdetailsID: "cinID3",
                DateOfInitialCPC: pd.NA,
                ICPCnotRequired: "1",
            },
        ]
    )
    # if rule requires columns containing date values, convert those columns to datetime objects first. Do it here in the test_validate function, not above.
    sample_s47[DateOfInitialCPC] = pd.to_datetime(
        sample_s47[DateOfInitialCPC],
        format="%d/%m/%Y",
        errors="coerce",
    )

    # Run rule function passing in our sample data
    result = run_rule(validate, {Section47: sample_s47})

    # Use .type3_issues to check for the result of .push_type3_issues() which you used above.
    issues_list = result.type3_issues
    # Issues list contains the objects pushed in their respective order. Since push_type3 was only used once, there will be one object in issues_list.
    assert len(issues_list) == 1

    issues = issues_list[0]

    issue_table = issues.table
    assert issue_table == Section47

    issue_columns = issues.columns
    assert issue_columns == [DateOfInitialCPC]

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
                    pd.NaT,
                ),
                "ROW_ID": [0, 1],
            },
            {
                "ERROR_ID": (
                    "child2",
                    "cinID2",
                    pd.NaT,
                ),
                "ROW_ID": [4, 5],
            },
        ]
    )
    assert issue_rows.equals(expected_df)

    # Check that the rule definition is what you wrote in the context above.

    # replace '8839' with the rule code and put the appropriate message in its place too.
    assert result.definition.code == "8839"
    assert (
        result.definition.message
        == "Within one CINDetails group there are 2 or more open S47 Assessments"
    )
