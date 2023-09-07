from typing import Mapping

import pandas as pd

from cin_validator.rule_engine import CINTable, RuleContext, rule_definition
from cin_validator.test_engine import run_rule
from cin_validator.utils import make_census_period

ChildProtectionPlans = CINTable.ChildProtectionPlans
CPPstartDate = ChildProtectionPlans.CPPstartDate
LAchildID = ChildProtectionPlans.LAchildID

Header = CINTable.Header
ReferenceDate = Header.ReferenceDate

CINdetails = CINTable.CINdetails
DateOfInitialCPC = CINdetails.DateOfInitialCPC

Section47 = CINTable.Section47


# define characteristics of rule
@rule_definition(
    code="2883",
    # module is table that seems central to the condition.
    module=CINTable.ChildProtectionPlans,
    message="There are more child protection plans starting than initial conferences taking place",
    affected_fields=[CPPstartDate, DateOfInitialCPC],
)
def validate(
    data_container: Mapping[CINTable, pd.DataFrame], rule_context: RuleContext
):
    df_cpp = data_container[ChildProtectionPlans]
    df_cin = data_container[CINdetails]
    df_47 = data_container[Section47]

    df_ref = data_container[Header]
    ref_date_series = df_ref[ReferenceDate]
    collection_start, collection_end = make_census_period(ref_date_series)

    # Within a Local Authority, count the number of <CPPStartDate> (N00105) where a date is present and within [Period_of_Census]. This value should be less than or equal to the sum of:
    # a) the count of <DateOfInitialCPC> (N00110) on CIN Details module where a date is present and within [Period_of_Census], plus
    # b) the count of <DateOfInitialCPC> on the S47 module where a date is present and within [Period_of_Census].

    # filter and count CPPstartDate
    present_cpp = df_cpp[CPPstartDate].notna()
    within_census_cpp = (df_cpp[CPPstartDate] >= collection_start) & (
        df_cpp[CPPstartDate] <= collection_end
    )
    df_cpp = df_cpp[present_cpp & within_census_cpp]
    num_cpp = len(df_cpp)

    # filter and count DateOfInitialCPC in CINdetails
    present_cin = df_cin[DateOfInitialCPC].notna()
    within_census_cin = (df_cin[DateOfInitialCPC] >= collection_start) & (
        df_cin[DateOfInitialCPC] <= collection_end
    )
    df_cin = df_cin[present_cin & within_census_cin]
    num_cin = len(df_cin)

    # filter and count DateOfInitialCPC in Section47
    present_47 = df_47[DateOfInitialCPC].notna()
    within_census_47 = (df_47[DateOfInitialCPC] >= collection_start) & (
        df_47[DateOfInitialCPC] <= collection_end
    )
    df_47 = df_47[present_47 & within_census_47]
    num_47 = len(df_47)

    if num_cpp > (num_cin + num_47):
        rule_context.push_la_level(
            rule_context.definition.code, rule_context.definition.message
        )
    else:
        pass


def test_validate():
    sample_header = pd.DataFrame(
        [{ReferenceDate: "31/03/2001"}]  # collection_start is 01/04/2000
    )
    sample_cpp = pd.DataFrame(
        [  # num_cpp = 3
            {
                LAchildID: "child1",
                CPPstartDate: "26/05/2000",
            },
            {
                LAchildID: "child1",  # checks that values are considered independently and no grouping is done.
                CPPstartDate: "27/06/2000",
            },
            {
                LAchildID: "child2",
                CPPstartDate: "26/05/2004",  # not considered: out of period of census
            },
            {
                LAchildID: "child3",
                CPPstartDate: "26/05/2000",
            },
            {
                LAchildID: "child1",
                CPPstartDate: pd.NA,  # ignore: absent
            },
        ]
    )

    sample_cin = pd.DataFrame(
        [  # num_cin = 0
            {
                LAchildID: "child2",
                DateOfInitialCPC: "26/05/2004",  # not considered: out of period of census
            },
            {
                LAchildID: "child1",
                DateOfInitialCPC: pd.NA,  # ignore: absent
            },
        ]
    )

    sample_section47 = pd.DataFrame(
        [  # num_47 = 2
            {
                LAchildID: "child1",
                DateOfInitialCPC: "26/05/2000",
            },
            {
                LAchildID: "child1",  # checks that values are considered independently and no grouping is done.
                DateOfInitialCPC: "27/06/2000",
            },
        ]
    )

    sample_cpp[CPPstartDate] = pd.to_datetime(
        sample_cpp[CPPstartDate], format="%d/%m/%Y", errors="coerce"
    )
    sample_cin[DateOfInitialCPC] = pd.to_datetime(
        sample_cin[DateOfInitialCPC], format="%d/%m/%Y", errors="coerce"
    )
    sample_section47[DateOfInitialCPC] = pd.to_datetime(
        sample_section47[DateOfInitialCPC], format="%d/%m/%Y", errors="coerce"
    )

    result = run_rule(
        validate,
        {
            ChildProtectionPlans: sample_cpp,
            CINdetails: sample_cin,
            Section47: sample_section47,
            Header: sample_header,
        },
    )

    issues = result.la_issues
    assert issues == (
        "2883",
        "There are more child protection plans starting than initial conferences taking place",
    )
