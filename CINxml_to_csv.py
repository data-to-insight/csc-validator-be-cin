import pandas as pd
import xml.etree.ElementTree as ET

# parse() processes files while fromstring() processes strings
# fulltree = ET.parse("CIN_Census_2021.xml")
fulltree = ET.parse("fake_CIN_data.xml")

# Get the outermost tag as a starting point
message = fulltree.getroot()
# the components are [Header, Children]
children = message[1]

all_children = {}  # list of group_generals of each child. One dict added per child.
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

    group_generals = {}  # dict of dicts containing tag elements that have no subtags.

    for group in child:
        temp_dict = {}  # intermediate storage for processing tasks
        # groups are: ChildCharacteristics, ChildIdentifiers, CINdetails
        group.set("child_count", child_count)
        group_count = group.tag + "Count"
        group_counts[group_count] += 1

        group.set(group_count, group_counts[group_count])
        print(f"--------{group.tag, group.attrib}-------------")
        # create dictionary start point containing all unique identifiers per group
        table_name = group.tag + "Table"
        temp_dict[table_name] = group.attrib

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
                temp_dict[table_name][element.tag] = element.text
                """if table_name in temp_dict: # if an instance of this group has been seen before,...
                    temp_dict[table_name] = temp_dict[table_name].append(temp_dict[table_name]) 
                else:
                    temp_dict[table_name] = [].append(temp_dict[table_name])"""

        # each value in group_generals should be a list of dictionaries; one dict per occurence of that group.
        ## end: for element in group. Here, temp_dict for one group has been fully created.

        #  Convert dict value to a list containing a dict.
        temp_dict = {k: [v] for k, v in temp_dict.items()}
        """temp_dict["ChildIdentifiersTable"].extend(temp_dict["ChildIdentifiersTable"])
        # print(temp_dict)
        print(pd.DataFrame.from_dict(temp_dict["ChildIdentifiersTable"]))
        break"""
        for k, v in temp_dict.items():
            # check whether table name exists in group_generals already
            if k in group_generals:
                # if it exists already, add to list
                # group_generals[k] = [group_generals[k]]
                group_generals[k].extend(v)
                # merge two lists. append would have created a list in a list.
            else:
                # if it doesn't exist, start list
                group_generals[k] = v

    for k, v in group_generals.items():
        group_generals[k] = pd.DataFrame(group_generals[k])
        # add it to the data of all the other children present
        if k in all_children:
            all_children[k] = pd.concat([all_children[k], group_generals[k]])
        else:
            all_children[k] = group_generals[k]
## end: for child in children.

for table_name, table_df in all_children.items():
    print(f"************* {table_name} **************")
    print(table_df)
    """
    At this point, CSV file strings can be generated using
    v.to_csv()
    and sent to the frontend.
    """
# TODO make this into a class object