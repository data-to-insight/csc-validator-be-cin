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
# when there is more than one CINdetails block in a child, multiple rows are generated as expected.
"""
************* ChildIdentifiersTable **************
   child_count  ChildIdentifiersCount    LAchildID            UPN      FormerUPN UPNunknown PersonBirthDate ExpectedPersonBirthDate GenderCurrent PersonDeathDate
0            1                      1  DfEX0000001  A123456789123  X98765432123B        UN3      1965-03-27              1066-04-13             1      1980-10-08
************* ChildCharacteristicsTable **************
   child_count  ChildCharacteristicsCount Ethnicity            Disabilities
0            1                          1      WBRI  \n
************* CINdetailsTable **************
   child_count  CINdetailsCount CINreferralDate ReferralSource PrimaryNeedCode CINclosureDate ReasonForClosure DateOfInitialCPC ReferralNFA
0            1                1      1970-10-06             1A              N4     1971-02-27              RC1       1970-12-06           0
1            1                2      1970-10-06             1A              N4     1971-02-27              RC1       1970-12-06           0
"""
# group table generation works fully even when there are multiple children and repeting blocks within them.
"""
************* ChildIdentifiersTable **************
    child_count  ChildIdentifiersCount        LAchildID            UPN PersonBirthDate GenderCurrent PersonDeathDate
0             1                      1  RND000215205141  A850728973744      2019-12-06             1             NaN
0             2                      1  RND000824303014  A141396438491      2011-04-27             9             NaN
0             3                      1  RND000750143123  A929946861554      2017-06-06             1             NaN
0             4                      1  RND000909164501  A612330267292      2014-10-03             0             NaN
0             5                      1  RND000382171815  A604459366806      2019-09-25             2             NaN
..          ...                    ...              ...            ...             ...           ...             ...
0           328                      1  RND000112711501  A465246916125      2010-07-07             2             NaN
0           329                      1  RND000513120794  A540014111973      2018-08-14             2             NaN
0           330                      1  RND000541643134  A549582689058      2021-12-09             1             NaN
0           331                      1  RND000404939452  A889492349196      2013-07-23             2             NaN
0           332                      1  RND000589802835  A877624860226      2021-10-25             9             NaN

[332 rows x 7 columns]
************* ChildCharacteristicsTable **************
    child_count  ChildCharacteristicsCount Ethnicity  Disabilities
0             1                          1      WIRT           NaN
0             2                          1      WROM           NaN
0             3                          1      AOTH           NaN
0             4                          1      MWBC           NaN
0             5                          1      APKN  \n
..          ...                        ...       ...           ...
0           328                          1      WOTH  \n
0           329                          1      WROM           NaN
0           330                          1      BCRB           NaN
0           331                          1      AIND           NaN
0           332                          1      WBRI           NaN

[332 rows x 4 columns]
************* CINdetailsTable **************
    child_count  CINdetailsCount CINreferralDate ReferralSource PrimaryNeedCode CINclosureDate ReasonForClosure DateOfInitialCPC ReferralNFA
0             1                1      2020-07-13              4              N6     2021-06-11              RC2       2020-09-29        true
0             2                1      2013-10-25              6              N8     2014-01-01              RC1       2013-12-13        true
1             2                2      2017-10-22             3A              N9            NaN              NaN       2017-11-05        true
0             3                1      2020-01-14             2A              N4     2020-06-16              RC4       2020-03-20        true
1             3                2      2022-02-07              9              N9            NaN              NaN       2022-05-01        true
..          ...              ...             ...            ...             ...            ...              ...              ...         ...
0           330                1      2022-03-29             1C              N5     2022-04-04              RC2              NaN        true
0           331                1      2019-02-06             1A              N6     2021-11-25              RC7       2019-03-05        true
1           331                2      2022-05-03             5B              N1     2022-05-21              RC4       2022-05-20        true
2           331                3      2022-06-02             1A              N8            NaN              NaN       2022-07-13        true
0           332                1      2022-01-10              4              N6            NaN              NaN       2022-03-28        true

[402 rows x 9 columns]
"""
