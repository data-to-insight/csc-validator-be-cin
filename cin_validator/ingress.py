import xml.etree.ElementTree as ET
import pandas as pd
from cin_validator.rule_engine import CINTable

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

    def create_Header(child):
        pass
    def create_ChildIdentifiers(child):
        # pick out the values of relevant columns
        # add to the global attribute
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

XMLtoCSV(message)