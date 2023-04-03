from typing import Mapping

import pandas as pd

from cin_validator.rule_engine import CINTable, RuleContext, RuleType, rule_definition
from cin_validator.test_engine import run_rule

# Get tables and columns of interest from the CINTable object defined in rule_engine/__api.py

ChildIdentifiers = CINTable.ChildIdentifiers
CINdetails = CINTable.CINdetails
LAchildID = ChildIdentifiers.LAchildID


PersonBirthDate = ChildIdentifiers.PersonBirthDate
CINclosureDate = CINdetails.CINclosureDate

# Reference date in header is needed to define the period of census.
Header = CINTable.Header
ReferenceDate = Header.ReferenceDate


# define characteristics of rule
@rule_definition(
    # write the rule code here, in place of 8775Q
    code="8775Q",
    rule_type=RuleType.QUERY,
    # replace ChildIdentifiers with the value in the module column of the excel sheet corresponding to this rule .
    # Note that even if multiple tables are involved, one table will be named in the module column.
    module=CINTable.ChildIdentifiers,
    # replace the message with the corresponding value for this rule, gotten from the excel sheet.
    message="Please check and either amend data or provide a reason: Child is over 25 years old",
    # The column names tend to be the words within the < > signs in the github issue description.
    affected_fields=[
        PersonBirthDate,
        CINclosureDate,
    ],
)
def validate(
    data_container: Mapping[CINTable, pd.DataFrame], rule_context: RuleContext
):
    # PREPARING DATA

    df_ci = data_container[ChildIdentifiers].copy()
    df_cin = data_container[CINdetails].copy()

    # Before you begin, rename the index so that the initial row positions can be kept intact.
    df_ci.index.name = "ROW_ID"
    df_cin.index.name = "ROW_ID"

    # Resetting the index causes the ROW_IDs to become columns of their respective DataFrames
    # so that they can come along when the merge is done.
    df_ci.reset_index(inplace=True)
    df_cin.reset_index(inplace=True)

    # get collection period
    df_ref = data_container[Header]
    ref_date = df_ref[ReferenceDate].iloc[0]

    # lOGIC
    # <PersonBirthDate> (N00066) is before (<ReferenceDate> (N00603) minus 25 years) AND
    # {<CINClosureDate> (N00102) is after (<PersonBirthDate> plus 25 years) OR <CINClosureDate> is NULL}
    over_25 = df_ci[PersonBirthDate] < (ref_date - pd.DateOffset(years=25))
    df_ci = df_ci[over_25]

    merged_df = df_ci.merge(
        df_cin,
        on=[
            LAchildID,
        ],
        suffixes=["_ci", "_cin"],
    )

    condition = (
        (
            merged_df["CINclosureDate"]
            > (merged_df[PersonBirthDate] + pd.DateOffset(years=25))
        )
        | (merged_df["CINclosureDate"].isna())
        | (merged_df["CINclosureDate"] == "NULL")
    )

    merged_df = merged_df[condition].reset_index()

    # create an identifier for each error instance.
    merged_df["ERROR_ID"] = tuple(zip(merged_df[LAchildID], merged_df[PersonBirthDate]))

    # we can now map the suffixes columns to their corresponding source tables such that the failing ROW_IDs and ERROR_IDs exist per table.
    df_ci_issues = (
        df_ci.merge(merged_df, left_on="ROW_ID", right_on="ROW_ID_ci")
        .groupby("ERROR_ID", group_keys=False)["ROW_ID"]
        .apply(list)
        .reset_index()
    )
    df_cin_issues = (
        df_cin.merge(merged_df, left_on="ROW_ID", right_on="ROW_ID_cin")
        .groupby("ERROR_ID", group_keys=False)["ROW_ID"]
        .apply(list)
        .reset_index()
    )

    # Ensure that you maintain the ROW_ID, and ERROR_ID column names which are shown above. They are keywords in this project.
    rule_context.push_type_2(
        table=ChildIdentifiers, columns=[PersonBirthDate], row_df=df_ci_issues
    )
    rule_context.push_type_2(
        table=CINdetails, columns=[CINclosureDate], row_df=df_cin_issues
    )


def test_validate():
    # Create some sample data such that some values pass the validation and some fail.
    sample_ci = pd.DataFrame(
        [
            {"LAchildID": "child1", "PersonBirthDate": "01/01/1880"},
            {"LAchildID": "child2", "PersonBirthDate": "01/01/1880"},
            {
                "LAchildID": "child3",
                "PersonBirthDate": "01/01/2000",
            },  # ignore: not 25 years before refdate
            {
                "LAchildID": "child4",
                "PersonBirthDate": "01/01/1800",
            },  # ignore: CINclosureDate not up to 25years after birthdate
        ]
    )
    sample_cin_details = pd.DataFrame(
        [
            {  # 0 fail
                "LAchildID": "child1",
                "CINclosureDate": "01/01/2000",
            },
            {  # 1 fail
                "LAchildID": "child2",
                "CINclosureDate": "NULL",
            },
            {  # 2 pass
                "LAchildID": "child2",
                "CINclosureDate": "26/10/1890",
            },
            {  # 3 pass
                "LAchildID": "child3",
                "CINclosureDate": "26/10/1999",
            },
            {  # 4 pass
                "LAchildID": "child4",
                "CINclosureDate": "26/10/1804",
            },
        ]
    )
    # if rule requires columns containing date values, convert those columns to datetime objects first. Do it here in the test_validate function, not above.
    sample_ci["PersonBirthDate"] = pd.to_datetime(
        sample_ci["PersonBirthDate"], format="%d/%m/%Y", errors="coerce"
    )
    sample_cin_details["CINclosureDate"] = pd.to_datetime(
        sample_cin_details["CINclosureDate"], format="%d/%m/%Y", errors="coerce"
    )
    sample_header = pd.DataFrame(
        [
            {
                ReferenceDate: pd.to_datetime(
                    "31/03/2001", format="%d/%m/%Y", errors="coerce"
                )
            }
        ]  # the census start date here will be 01/04/2000
    )

    # Run the rule function, passing in our sample data.
    result = run_rule(
        validate,
        {
            ChildIdentifiers: sample_ci,
            CINdetails: sample_cin_details,
            Header: sample_header,
        },
    )

    # Use .type2_issues to check for the result of .push_type2_issues() which you used above.
    issues_list = result.type2_issues
    assert len(issues_list) == 2
    # the function returns a list on NamedTuples where each NamedTuple contains (table, column_list, df_issues)
    issues = issues_list[1]

    # get table name and check it. Replace Section47 with the name of your table.
    issue_table = issues.table
    assert issue_table == CINdetails

    issue_columns = issues.columns
    assert issue_columns == [CINclosureDate]

    # check that the location linking dataframe was formed properly.
    issue_rows = issues.row_df
    # replace 2 with the number of failing points you expect from the sample data.
    assert len(issue_rows) == 2
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
                    "child1",  # ChildID
                    pd.to_datetime("01/01/1880", format="%d/%m/%Y", errors="coerce"),
                ),
                "ROW_ID": [0],
            },
            {
                "ERROR_ID": (
                    "child2",
                    pd.to_datetime("01/01/1880", format="%d/%m/%Y", errors="coerce"),
                ),
                "ROW_ID": [1],
            },
        ]
    )
    assert issue_rows.equals(expected_df)

    # Check that the rule definition is what you wrote in the context above.

    # replace 8775Q with the rule code and put the appropriate message in its place too.
    assert result.definition.code == "8775Q"
    assert (
        result.definition.message
        == "Please check and either amend data or provide a reason: Child is over 25 years old"
    )
