import pandas as pd
import xml.etree.ElementTree as ET

# parse() processes files while fromstring() processes strings
fulltree = ET.parse("CIN_Census_2021.xml")
# fulltree = ET.parse('fake_CIN_data.xml')

# Get the outermost tag as a starting point
message = fulltree.getroot()
# the components are [Header, Children]
children = message[1]

group_generals = {}  # dict of dicts containing tag elements that have no subtags.
all_children = [] # list of group_generals of each child. One dict added per child.
child_count = 0
for child in children:
    child_count += 1
    child.set("child_count", child_count)
    print(f"***********child {child.attrib}*************")

    # TODO check that the unique groups never have a count>0. That is ChildIdentifiers and ChildCharacteristics.
    group_counts = {
        "ChildIdentifiersCount": 0,
        "ChildCharacteristicsCount": 0,
        "CINdetailsCount": 0,
    }
    for group in child:
        # groups are: ChildCharacteristics, ChildIdentifiers, CINdetails
        group.set("child_count", child_count)
        group_count = group.tag + "Count"
        group_counts[group_count] += 1

        group.set(group_count, group_counts[group_count])
        print(f"--------{group.tag, group.attrib}-------------")
        # create dictionary start point containing all unique identifiers per group
        table_name = group.tag + "Table"
        group_generals[table_name] = group.attrib

        element_counts = {
            "Disabilities": 0,
            "AssessmentsCount": 0,
            "CINPlanDatesCount": 0,
            "Section47Count": 0,
            "ChildProtectionPlansCount": 0,
        }
        for element in group:
            # example elements are: Assessments, CINPlanDates, Section47, ChildProtectionPlans
            element_count = element.tag + "Count"
            if element_count in element_counts:
                element_counts[element_count] += 1
                element.set("child_count", child_count)
                element.set(group_count, group_counts[group_count])
                element.set(element_count, element_counts[element_count])
                print(f"{element.tag}:{element.attrib}")

                if element.tag in ["Assessments", "ChildProtectionPlans"]:
                    sub_counts = {
                        "ReviewsCount": 0,
                        "FactorsIdentifiedAtAssessmentCount": 0,
                    }
                    for sub in element:
                        sub_count = sub.tag + "Count"
                        if sub_count in sub_counts:
                            sub_counts[sub_count] += 1

                            sub.set("child_count", child_count)
                            sub.set(group_count, group_counts[group_count])
                            sub.set(element_count, element_counts[element_count])
                            sub.set(sub_count, sub_counts[sub_count])
                            print(f"{sub.tag}:{sub.attrib}")
            else:
                # for example: CINreferralDate, ReferralSource, PrimaryNeedCode
                group_generals[table_name][element.tag] = [element.text]
    all_children.append(group_generals)
print("#################################################")
print(group_generals)
print("#################################################")

for name, values in group_generals.items():
    df1 = pd.DataFrame(values)
    print(f"############# {name} ############")
    print(df1)
