from typing import Mapping

import pandas as pd

from cin_validator.rule_engine import (
    CINTable,
    IssueLocator,
    RuleContext,
    rule_definition,
)
from cin_validator.test_engine import run_rule

ChildIdentifiers = CINTable.ChildIdentifiers
UPN = ChildIdentifiers.UPN


@rule_definition(
    code="1530",
    module=CINTable.ChildIdentifiers,
    message="UPN invalid (characters 2-4 not a recognised LA code)",
    affected_fields=[UPN],
)
def validate(
    data_container: Mapping[CINTable, pd.DataFrame], rule_context: RuleContext
):
    df = data_container[ChildIdentifiers]
    """
    If <UPN> (N00001) present then characters 2-4 of <UPN> must be a valid post April 1998 LA code 
    or a recognised ‘pseudo LA’ code 

    001-005, 201-213, 301-320, 330-336, 340-344, 350-359, 370-373, 380-384, 390-394, 420, 660-681, 
    701-708, 800-803, 805-808, 810-813, 815, 816, 820- 823, 825, 826, 830, 831, 835-837, 838-839, 
    840, 841, 845, 846, 850-852, 855-857, 860, 861, 865-896, 908, 909, 916, 919, 921, 925, 
    926, 928, 929, 931, 933, 935-938, 940-941
    """
    LA_list = []
    LA_list.extend("00" + str(x) for x in range(1, 6))
    LA_list.extend(range(201, 214))
    LA_list.extend(range(301, 321))
    LA_list.extend(range(330, 337))
    LA_list.extend(range(340, 345))
    LA_list.extend(range(350, 360))
    LA_list.extend(range(370, 374))
    LA_list.extend(range(380, 385))
    LA_list.extend(range(390, 395))
    LA_list.extend(range(660, 682))
    LA_list.extend(range(701, 709))
    LA_list.extend(range(800, 804))
    LA_list.extend(range(805, 809))
    LA_list.extend(range(810, 814))
    LA_list.extend(range(820, 824))
    LA_list.extend(range(835, 840))
    LA_list.extend(range(850, 853))
    LA_list.extend(range(855, 858))
    LA_list.extend(range(865, 897))
    LA_list.extend(range(935, 939))
    LA_list.extend(range(940, 942))
    LA_list.extend(
        [
            420,
            815,
            816,
            825,
            826,
            830,
            831,
            840,
            841,
            845,
            846,
            860,
            861,
            908,
            909,
            916,
            919,
            921,
            925,
            926,
            928,
            929,
            931,
            933,
        ]
    )

    LA_list = [str(x) for x in LA_list]

    df.reset_index(inplace=True)
    df2 = df[["index", "UPN"]]
    df2 = df2[(df2["UPN"].str.len() == 13) & df2["UPN"].notna()]
    df2["C2_to_C4"] = df2["UPN"].str[1:4]
    df2 = df2[~df2["C2_to_C4"].isin(LA_list)]

    failing_indices = df2.set_index("index").index

    rule_context.push_issue(table=ChildIdentifiers, field=UPN, row=failing_indices)


def test_validate():
    child_identifiers = pd.DataFrame(
        {
            "UPN": [
                # These should pass
                "A38100178301",  # 0 In LA list
                pd.NA,  # 1
                "H003278544154",  # 2 In LA list
                "R34",  # 3 Nonsense
                # These should fail
                "R421962919251",  # 4 Not in LA list
                "X817558133462",  # 5 Not in LA list
                "ASFFAGSVSV123",  # 6 Not in LA list, not numeric.
            ]
        }
    )

    result = run_rule(validate, {ChildIdentifiers: child_identifiers})

    issues = list(result.issues)

    assert len(issues) == 3

    assert issues == [
        IssueLocator(CINTable.ChildIdentifiers, UPN, 4),
        IssueLocator(CINTable.ChildIdentifiers, UPN, 5),
        IssueLocator(CINTable.ChildIdentifiers, UPN, 6),
    ]

    assert result.definition.code == "1530"
    assert (
        result.definition.message
        == "UPN invalid (characters 2-4 not a recognised LA code)"
    )
