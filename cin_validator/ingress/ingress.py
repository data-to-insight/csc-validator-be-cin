import xml.etree.ElementTree as ET
from cin_validator.rule_engine import rule_definition, CINTable

# initialize all data sets as empty dataframes with columns names
# whenever a child is created, it should add a row to each table where it exists.
# tables should be attributes of a class that are accessible to the methods in create_child.
class XMLtoCSV():
    # define column names from CINTable object.
    pass
# for each table, column names should attempt to find their value in the child.
# if not found, they should assign themselves to NaN

    def create_child(self, child):
        self.create_Header(child)
        self.create_ChildIdentifiers(child)
        self.create_ChildCharacteristics(child)
        # CINdetailsID needed
        self.create_CINdetails(child)
        self.create_Asessments(child)
        self.create_CINplanDates(child)
        self.create_Section47(child)
        # CINdetails and CPPID needed
        self.create_ChildProtectionPlans(child)
        self.create_Reviews(child)

    def create_Header(child):
        pass
    def create_ChildIdentifiers(child):
        pass
    def create_ChildCharacteristics(child):
        pass
    # CINdetailsID needed
    def create_CINdetails(child):
        pass
    def create_Asessments(child):
        pass
    def create_CINplanDates(child):
        pass
    def create_Section47(child):
        pass
    # CINdetails and CPPID needed
    def create_ChildProtectionPlans(child):
        pass
    def create_Reviews(child):
        pass

# TODO make file path os-independent
fulltree = ET.parse("./fake_data/CIN_Census_2021.xml")
# fulltree = ET.parse("./fake_data/fake_CIN_data.xml")

message = fulltree.getroot()

# create header table
header = message.find("Header")

# create child tables
children = message.find("Children").findall("Child")
for child in children:
    pass
    # create_child(child)

for thing in CINTable:
    print(thing)