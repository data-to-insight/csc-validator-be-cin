This file groups rules that have similar logic.
# Rule Groups

#### - value must be present and equal to ...
100, 4180, 4220, 8500, 8568, 8640, 8650, 8910

#### -  compare variable to specific hardcoded value.
8617, 8730, 8750, 8790, 8897, 8905, 8899Q, 8866

#### - date value should be on or after
1103, 1105, 4008, 4011, 4012, 4015, 8520, 8535Q, 8555, 8565, 8608, 8630, 8720, 8915, 8920, 8925

#### - check date value relative to period of census
4013, 8545Q, 8620, 8696, 8715, 
  - Conditionally: 8736, 8740, 8930

#### - date value compared to reference value minus n days
8530, 8670Q

#### - UPN syntax
1510, 1530, 1540, 1550, 

#### - value should equal either this column or that column
multiple tables: 2885

#### - if something happens (value present/absent) in module, another thing shouldn't happen (value present/absent)
8805, 8810, 8840, 8841, 8873, 8869
multiple tables
8585Q, 8614, 8615, 8867
  - on any: 4001, 
  - must happen on all instances: 8868

#### - there must be only one in group
4004, 8794, 8839, 8896, 8935, 8898

#### - value should not exist between any other interval within group. replace missing ends with RefDate
8940, 8890, 8863
  - unless equal to something: 4016, 4017

#### - value must be unique in data
8510

#### - either this value or that value must be present but not both
8525

#### - filter by weekday-type of date
8870Q, 8875

# Sample rule structures per type.
### type0 stem: 
8500 - 1 column
### type1 stem:
8840 - multiple columns in the same table.
8925 - link-id explanation
### type2 stem:
2885 - multiple tables
8873 - merging a table on itself to compare values within group.
### type3 stem
4016 - intra group interval overlap checks
8896 - check duplication of values

# Notes
Since the Asssessments table has no ID to distinguish groups from each other, the AssessmentAuthorisationDate is used as a differentiator.