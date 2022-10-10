'''
Rule number: 1105
Module: Child protection plans
Rule details: Where present, the <CPPStartDate> (N00105) must be on or after the <CINReferralDate> (N00100)
Rule message: The child protection plan start date cannot be before the referral date

'''
from re import M
from typing import Mapping

import pandas as pd

from cin_validator.rule_engine import rule_definition, CINTable, RuleContext
from cin_validator.rule_engine import IssueLocator
from cin_validator.test_engine import run_rule

# Get tables and columns of interest from the CINTable object defined in rule_engine/__api.py

ChildProtectionPlans = CINTable.ChildProtectionPlans
CINDetails = CINTable.CINdetails
CPPstartDate = ChildProtectionPlans.CPPstartDate
CINreferralDate = CINDetails.CINreferralDate

# define characteristics of rule
@rule_definition(
    code = 1105,
    module = CINTable.ChildProtectionPlans,
    message = "The child protection plan start date cannot be before the referral date",
    affected_fields = [CPPstartDate],
)

def validate(
    data_container: Mapping[CINTable, pd.DataFrame], rule_context: RuleContext
):
    df_CPP = data_container[ChildProtectionPlans]
    df_CIN = data_container[CINDetails]

    # <CPPStartDate> (N00105) must be on or after the <CINReferralDate> (N00100)
    # Convert columns to dates
    df_CPP['CPPstartDate'] = pd.to_datetime(df_CPP['CPPstartDate'], format='%d-%m-%Y', errors = 'coerce')
    df_CIN['CINreferralDate'] = pd.to_datetime(df_CIN['CINreferralDate'], format='%d-%m-%Y', errors = 'coerce')
    print(df_CPP)
    print(df_CIN)

    df_CPP = df_CPP[["LAchildID", "CINdetailsID", "CPPstartDate"]]
    df_CIN = df_CIN[["LAchildID", "CINdetailsID", "CINreferralDate"]]
    # Join 2 tables together

    df = pd.merge(df_CPP, df_CIN, on = ['LAchildID', 'CINdetailsID'])
    print(df)
    # Return those where dates don't align
    failing_indices = df[df['CINreferralDate'] > df['CPPstartDate']].index
    print(failing_indices)

    rule_context.push_issue(
        table = ChildProtectionPlans, field = CPPstartDate, row = failing_indices
    )



def test_validate():
    # Create some sample data such that some values pass the validation and some fail.
    # CPP table elements
    LAid_cpp = ['1', '2', '3']
    CINid_cpp = ['3', '4', '5']
    CPP_start = ['01-09-2022', '30-07-2021', '24-03-2004']

    CPP_dummy_data = pd.DataFrame({"LAchildID": LAid_cpp, "CINdetailsID": CINid_cpp, "CPPstartDate": CPP_start})

    # CIN table elements
    LAid_cin = ['1', '2', '3']
    CINid_cin = ['3', '4', '5']
    referral = ['01-09-2022', '21-03-2009', '20-01-2020']

    CIN_dummy_data = pd.DataFrame({"LAchildID": LAid_cin, "CINdetailsID": CINid_cin, "CINreferralDate": referral})


    # Run rule function passing in our sample data
    result = run_rule(validate, {ChildProtectionPlans: CPP_dummy_data, CINDetails: CIN_dummy_data})

    # The result contains a list of issues encountered
    issues = list(result.issues)
    # replace 2 with the number of failing points you expect from the sample data.
    assert len(issues) == 1
    # The last numbers represent the index values where you expect the sample data to fail the validation check.
    assert issues == [
        IssueLocator(CINTable.ChildProtectionPlans, CPPstartDate, 2),

    ]

    assert result.definition.code == 1105
    assert result.definition.message == "The child protection plan start date cannot be before the referral date"
