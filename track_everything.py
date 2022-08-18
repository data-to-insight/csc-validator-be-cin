# This file keeps track of the sample output produced by key changes to parts of the tool.

# assigning count IDs in CINxml_to_csv
"""
***********child {'child_count': 1}*************
--------('ChildIdentifiers', {'child_count': 1, 'ChildIdentifiersCount': 1})-------------
--------('ChildCharacteristics', {'child_count': 1, 'ChildCharacteristicsCount': 1})-------------
--------('CINdetails', {'child_count': 1, 'CINdetailsCount': 1})-------------
Assessments:{'child_count': 1, 'CINdetailsCount': 1, 'AssessmentsCount': 1}
FactorsIdentifiedAtAssessment:{'child_count': 1, 'CINdetailsCount': 1, 'AssessmentsCount': 1, 'FactorsIdentifiedAtAssessmentCount': 1}     
FactorsIdentifiedAtAssessment:{'child_count': 1, 'CINdetailsCount': 1, 'AssessmentsCount': 1, 'FactorsIdentifiedAtAssessmentCount': 2}     
FactorsIdentifiedAtAssessment:{'child_count': 1, 'CINdetailsCount': 1, 'AssessmentsCount': 1, 'FactorsIdentifiedAtAssessmentCount': 3}     
Assessments:{'child_count': 1, 'CINdetailsCount': 1, 'AssessmentsCount': 2}
FactorsIdentifiedAtAssessment:{'child_count': 1, 'CINdetailsCount': 1, 'AssessmentsCount': 2, 'FactorsIdentifiedAtAssessmentCount': 1}     
CINPlanDates:{'child_count': 1, 'CINdetailsCount': 1, 'CINPlanDatesCount': 1}
Section47:{'child_count': 1, 'CINdetailsCount': 1, 'Section47Count': 1}
ChildProtectionPlans:{'child_count': 1, 'CINdetailsCount': 1, 'ChildProtectionPlansCount': 1}
Reviews:{'child_count': 1, 'CINdetailsCount': 1, 'ChildProtectionPlansCount': 1, 'ReviewsCount': 1}
"""

# generate tables of group components that do not contain subcomponents.
"""
#################### group_generals for one child #############################
{'ChildIdentifiersTable': {'child_count': 1, 'ChildIdentifiersCount': 1, 'LAchildID': ['DfEX0000001'], 'UPN': ['A123456789123'], 'FormerUPN': ['X98765432123B'], 'UPNunknown': ['UN3'], 'PersonBirthDate': ['1965-03-27'], 'ExpectedPersonBirthDate': ['1066-04-13'], 'GenderCurrent': ['1'], 'PersonDeathDate': ['1980-10-08']}, 
    'ChildCharacteristicsTable': {'child_count': 1, 'ChildCharacteristicsCount': 1, 'Ethnicity': ['WBRI'], 'Disabilities': ['\n                    ']}, 
    'CINdetailsTable': {'child_count': 1, 'CINdetailsCount': 1, 'CINreferralDate': ['1970-10-06'], 'ReferralSource': ['1A'], 'PrimaryNeedCode': ['N4'], 'CINclosureDate': ['1971-02-27'], 'ReasonForClosure': ['RC1'], 'DateOfInitialCPC': ['1970-12-06'], 'ReferralNFA': ['0']}}
#################################################
############# ChildIdentifiersTable ############
   child_count  ChildIdentifiersCount    LAchildID            UPN      FormerUPN UPNunknown PersonBirthDate ExpectedPersonBirthDate GenderCurrent PersonDeathDate 
0            1                      1  DfEX0000001  A123456789123  X98765432123B        UN3      1965-03-27              1066-04-13             1      1980-10-08 
############# ChildCharacteristicsTable ############
   child_count  ChildCharacteristicsCount Ethnicity            Disabilities
0            1                          1      WBRI  \n
############# CINdetailsTable ############
   child_count  CINdetailsCount CINreferralDate ReferralSource PrimaryNeedCode CINclosureDate ReasonForClosure DateOfInitialCPC ReferralNFA
0            1                1      1970-10-06             1A              N4     1971-02-27              RC1       1970-12-06           0
"""
