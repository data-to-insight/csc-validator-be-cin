# TODO Possible tests for this file
# - Check that no generated table has more columns than expected.
# - Check that all generated tables have the required IDs.
# - Check that Header, ChildIdentifiers, and ChildCharacteristics tags do not repeat and have no duplicate subelements.

import pandas as pd
import xml.etree.ElementTree as ET

# TODO make this work with from cin_validator.utils import get_values
from utils import get_values

# initialize all data sets as empty dataframes with columns names
# whenever a child is created, it should add a row to each table where it exists.
# tables should be attributes of a class that are accessible to the methods in create_child.
class XMLtoCSV():
    # define column names from CINTable object.
    Header = pd.DataFrame(columns=["Collection", "Year", "ReferenceDate", "SourceLevel", "LEA", "SoftwareCode", "Release", "SerialNo", "DateTime"])
    ChildIdentifiers = pd.DataFrame(columns=["LAchildID", "UPN", "FormerUPN", "UPNunknown", "PersonBirthDate", "ExpectedPersonBirthDate", "GenderCurrent", "PersonDeathDate"])
    ChildCharacteristics = pd.DataFrame(columns=["LAchildID", "Ethnicity", ])
    Disabilities = pd.DataFrame(columns=["LAchildID", "Disability"])
    CINdetails = pd.DataFrame(columns=["LAchildID", "CINdetailsID", "CINreferralDate", "ReferralSource", "PrimaryNeedCode", "CINclosureDate", "ReasonForClosure", "DateOfInitialCPC", "ReferralNFA" ])
    Assessments = pd.DataFrame(columns=["LAchildID", "CINdetailsID", "AssessmentActualStartDate", "AssessmentInternalReviewDate", "AssessmentAuthorisationDate", "AssessmentFactors"]) 
    CINplanDates = pd.DataFrame(columns=["LAchildID", "CINdetailsID", "CINPlanStartDate", "CINPlanEndDate"])
    Section47 = pd.DataFrame(columns=["LAchildID", "CINdetailsID", "S47ActualStartDate", "InitialCPCtarget", "DateOfInitialCPC", "ICPCnotRequired"])
    ChildProtectionPlans = pd.DataFrame(columns=["LAchildID", "CINdetailsID", "CPPID", "CPPstartDate", "CPPendDate", "InitialCategoryOfAbuse", "LatestCategoryOfAbuse", "NumberOfPreviousCPP"]) 
    Reviews = pd.DataFrame(columns=["LAchildID", "CINdetailsID", "CPPID", "CPPreviewDate"])

    def __init__(self, root):
        header = root.find("Header")
        self.Header = self.create_Header(header)

        children = root.find("Children")
        for child in children.findall('Child'):
            self.create_child(child)

# for each table, column names should attempt to find their value in the child.
# if not found, they should assign themselves to NaN

    def create_child(self, child):
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

    # TODO set defaults so that if element is not found, program doesn't break.
    def create_Header(self, header):
        """One header exists in a CIN XML file"""

        header_dict = {}

        collection_details = header.find('CollectionDetails')
        collection_elements = ['Collection', 'Year', 'ReferenceDate']
        header_dict = get_values(collection_elements, header_dict, collection_details)

        source = header.find('Source')
        source_elements = ['SourceLevel', 'LEA', 'SoftwareCode', 'Release', 'SerialNo', 'DateTime']
        header_dict = get_values(source_elements, header_dict, source)

        header_df = pd.DataFrame.from_dict([header_dict])
        return header_df

    def create_ChildIdentifiers(self, child):
        """One ChildIdentifiers block exists per child in CIN XML"""
        # pick out the values of relevant columns
        # add to the global attribute
        identifiers_dict = {}

        identifiers = child.find('ChildIdentifiers')
        elements = self.ChildIdentifiers.columns
        identifiers_dict = get_values(elements, identifiers_dict, identifiers)
        
        identifiers_df = pd.DataFrame.from_dict([identifiers_dict])
        self.ChildIdentifiers = pd.concat([self.ChildIdentifiers, identifiers_df])

    def create_ChildCharacteristics(self, child):
        pass
    # CINdetailsID needed
    def create_CINdetails(self, child):
        pass
    def create_Asessments(self, child):
        pass
    def create_CINplanDates(self, child):
        pass
    def create_Section47(self, child):
        pass
    # CINdetails and CPPID needed
    def create_ChildProtectionPlans(self, child):
        pass
    def create_Reviews(self, child):
        pass

# TODO make file path os-independent
# fulltree = ET.parse("../fake_data/CIN_Census_2021.xml")
fulltree = ET.parse("../fake_data/fake_CIN_data.xml")

message = fulltree.getroot()

conv = XMLtoCSV(message)
print(conv.Header)