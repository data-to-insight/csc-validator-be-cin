from typing import Mapping

import pandas as pd

from cin_validator.rule_engine import rule_definition, CINTable, RuleContext
from cin_validator.rule_engine import IssueLocator
from cin_validator.test_engine import run_rule

# Get tables and columns of interest from the CINTable object defined in rule_engine/__api.py
# Replace ChildIdentifiers with the table name, and LAChildID with the column name you want.

ChildIdentifiers = CINTable.ChildIdentifiers
UPN = ChildIdentifiers.UPN

# define characteristics of rule
@rule_definition(
    # write the rule code here, in place of 8500
    code=1510,
    # replace ChildIdentifiers with the value in the module column of the excel sheet corresponding to this rule .
    module=CINTable.ChildIdentifiers,
    # replace the message with the corresponding value for this rule, gotten from the excel sheet.
    message="UPN invalid (wrong check letter at character 1)",
    # The column names tend to be the words within the < > signs in the github issue description.
    affected_fields=[UPN],
)
def validate(
    data_container: Mapping[CINTable, pd.DataFrame], rule_context: RuleContext
):
    # Replace ChildIdentifiers with the name of the table you need.
    df = data_container[ChildIdentifiers]

    # implement rule logic as descriped by the Github issue. Put the description as a comment above the implementation as shown.
    """
    <UPN> (N00001) if present must contain the correct check letter

    To calculate the check letter:

    1. Multiply the individual digits by their weights as follows:

    digit 2 by weight 2; digit 3 by weight 3; digit 4 by weight 4; digit 5 by weight 5; digit 6 by weight 6; digit 7 by weight 7; digit 8 by weight 8; digit 9 by weight 9; digit 10 by weight 10; digit 11 by weight 11; digit 12 by weight 12; digit 13 by weight 13.

    2. Sum the individual results, divide the total by 23, and take the remainder.

    3. Calculate the check letter from the result as follows:

    0  = A;  1  = B;  2  = C;  3  = D;  4  = E;  5 = F;  6 = G;
    7 = H;  8 = J;  9 = K;  10 = L;  11 = M;  12 = N;  13 = P;
    14 = Q;  15 = R;  16 = T;  17 = U;  18 = V;  19 = W;  20 = X;
    """
    df.reset_index(inplace=True)
    df2 = df[['index','UPN']]
    df2 = df2[(df2['UPN'].str.len() == 13) & df2['UPN'].notna()]
    df2['FIRST_CHAR'] = df['UPN'].str[:1]
    df2['LAST_C'] = df['UPN'].str[-12:]
    df2['LAST_C'] = df2['LAST_C'].apply(lambda x: int(x) if str(x).isdigit() else pd.NA)
    df2 = df2[df2['LAST_C'].notna()]
    for i in range(1,13):
        df2['C' + str(i)] = (df['UPN'].str[i].astype(int) * (i+1)) % 23
    df2['SUMMED'] = df2[['C1','C2','C3','C4','C5','C6','C7','C8','C9','C10','C11','C12']].sum(axis=1) % 23

    check_map = enumerate(list('ABCDEFGHJKLMNPQRTUVWX'))
    check_map = dict((i, j) for i, j in check_map)
    df2['CHECK_CHAR'] = df2['SUMMED'].map(check_map)

    df2 = df2[df2['CHECK_CHAR'].astype(str) != df2['FIRST_CHAR'].astype(str)]

    failing_indices = df2['index'].to_list()

    rule_context.push_issue(
        table=ChildIdentifiers, field=UPN, row=failing_indices
    )


def test_validate():
    # Create some sample data such that some values pass the validation and some fail.

    child_identifiers = pd.DataFrame([['W351201321005'], [pd.NA], ['W35320152101A']], columns=[UPN])

    # Run rule function passing in our sample data
    result = run_rule(validate, {ChildIdentifiers: child_identifiers})

    # The result contains a list of issues encountered
    issues = list(result.issues)
    # replace 2 with the number of failing points you expect from the sample data.
    assert len(issues) == 1
    # replace the table and column name as done earlier. 
    # The last numbers represent the index values where you expect the sample data to fail the validation check.
    assert issues == [
        IssueLocator(CINTable.ChildIdentifiers, UPN, 0),
    ]

    # Check that the rule definition is what you wrote in the context above.

    # replace 8500 with the rule code and put the appropriate message in its place too.
    assert result.definition.code == 1510
    assert result.definition.message == "UPN invalid (wrong check letter at character 1)"
