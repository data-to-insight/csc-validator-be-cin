from typing import Mapping

import pandas as pd

from cin_validator.rule_engine import CINTable, RuleContext, rule_definition
from cin_validator.test_engine import run_rule

# Get tables and columns of interest from the CINTable object defined in rule_engine/__api.py

CINplanDates = CINTable.CINplanDates
CINPlanStartDate = CINplanDates.CINPlanStartDate
CINPlanEndDate = CINplanDates.CINPlanEndDate
LAchildID = CINplanDates.LAchildID

# define characteristics of rule
@rule_definition(
    # write the rule code here, in place of 8500
    code=4011,
    # replace CINplanDates with the value in the module column of the excel sheet corresponding to this rule .
    module=CINTable.CINplanDates,
    # replace the message with the corresponding value for this rule, gotten from the excel sheet.
    message="CIN Plan End Date earlier than Start Date",
    # The column names tend to be the words within the < > signs in the github issue description.
    affected_fields=[CINPlanEndDate, CINPlanStartDate],
)
def validate(
    data_container: Mapping[CINTable, pd.DataFrame], rule_context: RuleContext
):
    # PREPARING DATA

    # Replace CINplanDates with the name of the table you need.
    df = data_container[CINplanDates]
    # Before you begine, rename the index so that the initial row positions can be kept intact.
    df.index.name = "ROW_ID"

    # <If present <CINPlanEndDate> (N00690) must be on or after the <CINPlanStartDate> (N00689)

    # Remove all rows with no end date
    df = df[~df[CINPlanEndDate].isna()]

    # Return rows where end date is prior to start dat
    condition1 = df[CINPlanEndDate] < df[CINPlanStartDate]

    # df with all rows meeting the conditions
    df_issues = df[condition1].reset_index()

    link_id = tuple(
        zip(df_issues[LAchildID], df_issues[CINPlanEndDate], df_issues[CINPlanStartDate])
    )
    df_issues["ERROR_ID"] = link_id
    df_issues = df_issues.groupby("ERROR_ID")["ROW_ID"].apply(list).reset_index()

    rule_context.push_type_1(
        table=CINplanDates, columns=[CINPlanEndDate, CINPlanStartDate], row_df=df_issues
    )


def test_validate():
    # Create some sample data such that some values pass the validation and some fail.
    cin_plan = pd.DataFrame(
        [
            {
                "LAchildID": "child1",
                "CINPlanEndDate": "26/05/2000",
                "CINPlanStartDate": "26/05/2000",
            },
            {
                "LAchildID": "child2",
                "CINPlanEndDate": "26/05/2000",
                "CINPlanStartDate": "26/05/2001",
                # fails, start after end
            }, 
            {
                "LAchildID": "child3",
                "CINPlanEndDate": "26/05/2000",
                "CINPlanStartDate": "26/05/1999",
            },  
            {
                "LAchildID": "child4",
                "CINPlanEndDate": "26/05/2000",
                "CINPlanStartDate": pd.NA,
                # pass, no requirement in this rule for a start date
            },
            {
                "LAchildID": "child6",
                "CINPlanEndDate": pd.NA,
                "CINPlanStartDate": pd.NA,
            },
        ]
    )
    # Conver to dates
    cin_plan[CINPlanEndDate] = pd.to_datetime(
        cin_plan[CINPlanEndDate], format="%d/%m/%Y", errors="coerce"
    )
    cin_plan[CINPlanStartDate] = pd.to_datetime(
        cin_plan[CINPlanStartDate], format="%d/%m/%Y", errors="coerce"
    )

    # Run rule function passing in our sample data
    result = run_rule(validate, {CINplanDates: cin_plan})

    # Use .type1_issues to check for the result of .push_type1_issues() which you used above.
    issues = result.type1_issues

    issue_table = issues.table
    assert issue_table == CINplanDates

    # check that the right columns were returned. Replace CINPlanEndDate and CINPlanStartDate with a list of your columns.
    issue_columns = issues.columns
    assert issue_columns == [CINPlanEndDate, CINPlanStartDate]

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
                    "child2",
                    pd.to_datetime("26/05/2000", format="%d/%m/%Y", errors="coerce"),
                    pd.to_datetime("26/05/2001", format="%d/%m/%Y", errors="coerce"),
                ),
                "ROW_ID": [1],
            },

        ]
    )
    assert issue_rows.equals(expected_df)

    # Check that the rule definition is what you wrote in the context above.

    assert result.definition.code == 4011
    assert (
        result.definition.message
        == "CIN Plan End Date earlier than Start Date"
    )
