from typing import Mapping

import pandas as pd

from cin_validator.rule_engine import (
    CINTable,
    IssueLocator,
    RuleContext,
    rule_definition,
)
from cin_validator.test_engine import run_rule

# Get tables and columns of interest from the CINTable object defined in rule_engine/__api.py

Assessments = CINTable.Assessments
LAchildID = Assessments.LAchildID
AssessmentFactors = Assessments.AssessmentFactors
CINdetailsID = Assessments.CINdetailsID
AssessmentAuthorisationDate = Assessments.AssessmentAuthorisationDate


# define characteristics of rule
@rule_definition(
    # write the rule code here
    code="8869",
    # replace Assessments with the value in the module column of the excel sheet corresponding to this rule .
    # Note that even if multiple tables are involved, one table will be named in the module column.
    module=CINTable.Assessments,
    # replace the message with the corresponding value for this rule, gotten from the excel sheet.
    message="The assessment factors code “21” cannot be used in conjunction with any other assessment factors.",
    # The column names tend to be the words within the < > signs in the github issue description.
    affected_fields=[
        AssessmentFactors,
    ],
)
def validate(
    data_container: Mapping[CINTable, pd.DataFrame], rule_context: RuleContext
):
    # PREPARING DATA

    df = data_container[Assessments].copy()

    # Before you begin, rename the index so that the initial row positions can be kept intact.
    df.index.name = "ROW_ID"

    # Resetting the index causes the ROW_IDs to become columns of their respective DataFrames
    # so that they can come along when the merge is done.
    df.reset_index(inplace=True)

    # lOGIC
    # If <AssessmentFactors> (N00181) = “21”, this must be the only <AssessmentFactors> (N00181) present.
    df_orig = df.copy()

    df = df[(df[AssessmentFactors].astype(str) == "21")]

    #  Merge tables
    df = df.merge(
        df_orig,
        how="left",
        on=[LAchildID, CINdetailsID, AssessmentAuthorisationDate],
        suffixes=["", "_orig"],
    )

    # Values that aren't 21 are now errors (using guidelines from rule 8790)
    df = df[df["AssessmentFactors_orig"] != "21"]

    failing_indices = df.set_index("ROW_ID_orig").index

    rule_context.push_issue(
        table=Assessments, field=AssessmentFactors, row=failing_indices
    )


def test_validate():
    # 0      #1      #2      #3     #4      #5      #6      #7
    ids = ["1", "1", "2", "3", "3", "4", "4", "5", "6", "6", "6"]
    cinid = ["1", "1", "2", "3", "3", "4", "4", "5", "6", "6", "6"]
    assessmentfactors = [
        "21",
        "AIND",  # id1, cinid1 fail. same assessment period has factor 21
        "NONE",
        pd.NA,
        "MOTH",
        "21",
        "AAAA",  # id4, cinid4 fail. same assessment period has factor 21
        "AA",
        "21",
        "14",  # ignored. not the same auth date as preceding.
        "15",
    ]
    # non-date placeholders can be used since this rule doesn't require date-object properties.
    assessmentauthorisationdate = [
        "1",
        "1",
        "2",
        "3",
        "3",
        "4",
        "4",
        "5",
        "2021-02-09",
        "2020-12-15",
        "2020-12-15",
    ]

    fake_df = pd.DataFrame(
        {
            "LAchildID": ids,
            "CINdetailsID": cinid,
            "AssessmentFactors": assessmentfactors,
            "AssessmentAuthorisationDate": assessmentauthorisationdate,
        }
    )

    result = run_rule(validate, {Assessments: fake_df})

    issues = list(result.issues)

    assert len(issues) == 2

    assert issues == [
        IssueLocator(CINTable.Assessments, AssessmentFactors, 1),
        IssueLocator(CINTable.Assessments, AssessmentFactors, 6),
    ]

    assert result.definition.code == "8869"
    assert (
        result.definition.message
        == "The assessment factors code “21” cannot be used in conjunction with any other assessment factors."
    )
