import pandas as pd

from .utils import get_values


# initialize all data sets as empty dataframes with columns names
# whenever a child is created, it should add a row to each table where it exists.
# tables should be attributes of a class that are accessible to the methods in create_child.
class XMLtoCSV:
    """
    A class to convert data input as XML into CSV/DataFrame format for validation. Uses
    ElementTree to parse the XML for each child, adding their data to relevant fields and tables.

    :param DataFrame Header: DataFrame of fields for the Header table for validation,
        to be populated with children's data from XML input.
    :param DataFrame ChildIdentifiers: DataFrame of fields for the ChildIdentifiers table for validation.
        to be populated with children's data from XML input.
    :param DataFrame ChildCharacteristics: DataFrame of fields for the ChildCharacterisitcs table for validation.
        to be populated with children's data from XML input.
    :param DataFrame Disabilities: DataFrame of fields for the Disabilities table for validation.
        to be populated with children's data from XML input.
    :param DataFrame CINdetails: DataFrame of fields for the CINdetails table for validation.
        to be populated with children's data from XML input.
    :param DataFrame Assessments: DataFrame of fields for the Assessments table for validation.
        to be populated with children's data from XML input.
    :param DataFrame CINplanDates: DataFrame of fields for the CINplanDates table for validation.
        to be populated with children's data from XML input.
    :param DataFrame Section47: DataFrame of fields for the Section47 table for validation.
        to be populated with children's data from XML input.
    :param DataFrame ChildProtectionPlans: DataFrame of fields for the ChildProtectionPlans table for validation.
        to be populated with children's data from XML input.
    :param DataFrame Reviews: DataFrame of fields for the Reviews table for validation.
        to be populated with children's data from XML input.
    :param list id_cols: List of columns containing IDs that can be used to merge tables.
    """

    # define column names from CINTable object.
    Header = pd.DataFrame(
        columns=[
            "Collection",
            "Year",
            "ReferenceDate",
            "SourceLevel",
            "LEA",
            "SoftwareCode",
            "Release",
            "SerialNo",
            "DateTime",
        ]
    )
    ChildIdentifiers = pd.DataFrame(
        columns=[
            "LAchildID",
            "UPN",
            "FormerUPN",
            "UPNunknown",
            "PersonBirthDate",
            "ExpectedPersonBirthDate",
            "GenderCurrent",
            "Sex",
            "PersonDeathDate",
        ]
    )
    ChildCharacteristics = pd.DataFrame(
        columns=[
            "LAchildID",
            "Ethnicity",
        ]
    )
    Disabilities = pd.DataFrame(columns=["LAchildID", "Disability"])

    CINdetails = pd.DataFrame(
        columns=[
            "LAchildID",
            "CINdetailsID",
            "CINreferralDate",
            "ReferralSource",
            "PrimaryNeedCode",
            "CINclosureDate",
            "ReasonForClosure",
            "DateOfInitialCPC",
            "ReferralNFA",
        ]
    )
    Assessments = pd.DataFrame(
        columns=[
            "LAchildID",
            "CINdetailsID",
            "AssessmentID",
            "AssessmentActualStartDate",
            "AssessmentInternalReviewDate",
            "AssessmentAuthorisationDate",
            "AssessmentFactors",
        ]
    )
    AssessmentFactorsList = pd.DataFrame(
        columns=[
            "LAchildID",
            "CINdetailsID",
            "AssessmentID",
            "AssessmentFactor",
        ]
    )
    CINplanDates = pd.DataFrame(
        columns=["LAchildID", "CINdetailsID", "CINPlanStartDate", "CINPlanEndDate"]
    )
    Section47 = pd.DataFrame(
        columns=[
            "LAchildID",
            "CINdetailsID",
            "S47ActualStartDate",
            "InitialCPCtarget",
            "DateOfInitialCPC",
            "ICPCnotRequired",
        ]
    )
    ChildProtectionPlans = pd.DataFrame(
        columns=[
            "LAchildID",
            "CINdetailsID",
            "CPPID",
            "CPPstartDate",
            "CPPendDate",
            "InitialCategoryOfAbuse",
            "LatestCategoryOfAbuse",
            "NumberOfPreviousCPP",
        ]
    )
    Reviews = pd.DataFrame(
        columns=["LAchildID", "CINdetailsID", "CPPID", "CPPreviewDate"]
    )

    id_cols = ["LAchildID", "CINdetailsID", "AssessmentID", "CPPID"]

    def __init__(self, root):
        """
        Initialises XMLtoCSV class, creates header, and iterates through input XML for every Child field
        in the Children field.

        :param xml root: root of the CIN XML data
        :returns: Generates 10 dataframes containing the child info from the CIN XML fed into it.
        """

        header = root.find("Header")
        self.Header = self.create_Header(header)

        children = root.find("Children")
        for child in children.findall("Child"):
            self.create_child(child)

    # for each table, column names should attempt to find their value in the child.
    # if not found, they should assign themselves to NaN

    def create_child(self, child):
        """
        For every child, use class methods to extract relevant fields from
        input XML and append its data to appropriate DataFrames.
        """

        # at the start of every child, reset the value of LAchildID
        self.LAchildID = None

        self.create_ChildIdentifiers(child)
        # LAchildID has been created. It can be used in the functions below.
        self.create_ChildCharacteristics(child)

        # CINdetailsID needed
        self.create_CINdetails(child)

        # CINdetails and CPPID needed
        self.create_ChildProtectionPlans(child)
        self.create_Reviews(child)

    # TODO get column names from the CINTable object instead of writing them out as strings?
    def create_Header(self, header):
        """Extracts header data from XML, run once as only one row is needed for the header.
        Exists once per census return.

        :param object header: The element with the "Header" tag in the input XML
        :returns: A DataFrame of the Header table extracted from input XML.
        :rtype: DataFrame
        """

        header_dict = {}

        collection_details = header.find("CollectionDetails")
        collection_elements = ["Collection", "Year", "ReferenceDate"]
        header_dict = get_values(collection_elements, header_dict, collection_details)

        source = header.find("Source")
        source_elements = [
            "SourceLevel",
            "LEA",
            "SoftwareCode",
            "Release",
            "SerialNo",
            "DateTime",
        ]
        header_dict = get_values(source_elements, header_dict, source)

        header_df = pd.DataFrame.from_dict([header_dict])
        return header_df

    def create_ChildIdentifiers(self, child):
        """
        Populates the ChildIdentifiers table. One ChildIdentifiers block exists per child in CIN XML

        :param xml child: 'child' element from the XML input. Each contains the full information per child.
        :returns: DataFrame of data for an individual child for the ChildIdentifiers Table.
        :rtype: DataFrame
        """

        # pick out the values of relevant columns
        # add to the global attribute
        identifiers_dict = {}

        identifiers = child.find("ChildIdentifiers")
        elements = self.ChildIdentifiers.columns
        identifiers_dict = get_values(elements, identifiers_dict, identifiers)

        self.LAchildID = identifiers_dict.get("LAchildID", pd.NA)

        identifiers_df = pd.DataFrame.from_dict([identifiers_dict])
        self.ChildIdentifiers = pd.concat(
            [self.ChildIdentifiers, identifiers_df], ignore_index=True
        )

    def create_ChildCharacteristics(self, child):
        """Populates the ChildCharacteristics table. One ChildCharacteristics block exists per child in CIN XML

        :param xml child: 'child' element from the XML input
        :returns: DataFrame of data for an individual child for the ChildCharacteristics Table.
        :rtype: DataFrame
        """
        # assign LAChild ID
        characteristics_dict = {"LAchildID": self.LAchildID}

        characteristics = child.find("ChildCharacteristics")
        columns = self.ChildCharacteristics.columns
        # select only columns whose values typically exist in this xml block.
        # remove id_cols which tend to come from other blocks or get generated at runtime.
        elements = list(set(columns).difference(set(self.id_cols)))

        characteristics_dict = get_values(
            elements, characteristics_dict, characteristics
        )

        characteristics_df = pd.DataFrame.from_dict([characteristics_dict])
        self.ChildCharacteristics = pd.concat(
            [self.ChildCharacteristics, characteristics_df], ignore_index=True
        )

        # The disabilities block for a child is found within a ChildCharacteristics block.
        self.create_Disabilities(characteristics)

    def create_Disabilities(self, characteristics):
        """
        Populates Disabilites table
        """
        disabilities_list = []
        columns = self.Disabilities.columns
        elements = list(set(columns).difference(set(self.id_cols)))
        # get the Disabilities block
        disabilities = characteristics.find("Disabilities")
        if disabilities is not None:
            # Only run this if a "Disabilities" xml block has been found
            for disability in disabilities:
                disability_dict = {
                    "LAchildID": self.LAchildID,
                }
                disability_dict = get_values(elements, disability_dict, disability)
                disability_dict["Disability"] = disability.text
                disabilities_list.append(disability_dict)

            disabilities_df = pd.DataFrame(disabilities_list)
            self.Disabilities = pd.concat(
                [self.Disabilities, disabilities_df], ignore_index=True
            )

    # CINdetailsID needed
    def create_CINdetails(self, child):
        """Populates the CINdetails table. Multiple CIN details blocks can exist in one child.

        :param xml child: 'child' element from the XML input
        :returns: DataFrame of data for an individual child for the CINdetails Table.
        :rtype: DataFrame
        """

        cin_details_list = []
        columns = self.CINdetails.columns
        elements = list(set(columns).difference(set(self.id_cols)))

        # TODO should we imitate DfE generator where the ID count for the first child is 1?
        self.CINdetailsID = 0

        cin_details = child.findall("CINdetails")
        for cin_detail in cin_details:
            self.CINdetailsID += 1
            cin_detail_dict = {
                "LAchildID": self.LAchildID,
                "CINdetailsID": self.CINdetailsID,
            }

            cin_detail_dict = get_values(elements, cin_detail_dict, cin_detail)
            cin_details_list.append(cin_detail_dict)

            # functions that should use the CINdetailsID before it is incremented.
            self.create_Assessments(cin_detail)
            self.create_CINplanDates(cin_detail)
            self.create_Section47(cin_detail)
            self.create_ChildProtectionPlans(cin_detail)

        cin_details_df = pd.DataFrame(cin_details_list)
        self.CINdetails = pd.concat(
            [self.CINdetails, cin_details_df], ignore_index=True
        )

    def create_Assessments(self, cin_detail):
        """Populates the assessments table. Multiple Assessments blocks can exist in one CINdetails block.

        :param xml child: 'child' element from the XML input
        :returns: DataFrame of data for an individual child for the Assessments Table.
        :rtype: DataFrame
        """

        assessments_list = []
        columns = self.Assessments.columns
        elements = list(set(columns).difference(set(self.id_cols)))

        self.AssessmentID = 0
        assessments = cin_detail.findall("Assessments")

        for assessment in assessments:
            self.AssessmentID += 1
            assessment_dict = {
                "LAchildID": self.LAchildID,
                "CINdetailsID": self.CINdetailsID,
                "AssessmentID": self.AssessmentID,
            }

            assessment_dict = get_values(elements, assessment_dict, assessment)

            # the get_values function will not find AssessmentFactors on that level so we retrieve these separately.
            assessment_factors = assessment.find("FactorsIdentifiedAtAssessment")
            assessment_factors_list = []
            assessment_columns = self.AssessmentFactorsList.columns
            assessment_elements = list(
                set(assessment_columns).difference(set(self.id_cols))
            )

            if assessment_factors is not None:
                # if statement handles the non-iterable NoneType that .find produces if the element is not present.
                for factor in assessment_factors:
                    assessment_factors_dict = {
                        "LAchildID": self.LAchildID,
                        "CINdetailsID": self.CINdetailsID,
                        "AssessmentID": self.AssessmentID,
                    }
                    assessment_factors_dict = get_values(
                        assessment_elements, assessment_factors_dict, factor
                    )
                    assessment_factors_dict["AssessmentFactor"] = factor.text
                    assessment_factors_list.append(assessment_factors_dict)
                assessment_factors_df = pd.DataFrame(assessment_factors_list)
                self.AssessmentFactorsList = pd.concat(
                    [self.AssessmentFactorsList, assessment_factors_df],
                    ignore_index=True,
                )
                assessment_dict["AssessmentFactors"] = assessment_factors_df[
                    "AssessmentFactor"
                ].tolist()

            assessments_list.append(assessment_dict)

        assessments_df = pd.DataFrame(assessments_list)
        self.Assessments = pd.concat(
            [self.Assessments, assessments_df], ignore_index=True
        )

    def create_CINplanDates(self, cin_detail):
        """
        Populates the CINplanDates table. Multiple CINplanDates blocks can exist in one CINdetails block.

        :param xml child: 'child' element from the XML input
        :returns: DataFrame of data for an individual child for the CINplanDates Table.
        :rtype: DataFrame
        """

        dates_list = []
        columns = self.CINplanDates.columns
        elements = list(set(columns).difference(set(self.id_cols)))

        dates = cin_detail.findall("CINPlanDates")
        for date in dates:
            date_dict = {
                "LAchildID": self.LAchildID,
                "CINdetailsID": self.CINdetailsID,
            }
            date_dict = get_values(elements, date_dict, date)
            dates_list.append(date_dict)

        dates_df = pd.DataFrame(dates_list)
        self.CINplanDates = pd.concat([self.CINplanDates, dates_df], ignore_index=True)

    def create_Section47(self, cin_detail):
        """
        Populates the Section47 table. Multiple Section47 blocks can exist in one CINdetails block.

        :param xml child: 'child' element from the XML input
        :returns: DataFrame of data for an individual child for the Section47 Table.
        :rtype: DataFrame
        """

        sections_list = []
        columns = self.Section47.columns
        elements = list(set(columns).difference(set(self.id_cols)))

        sections = cin_detail.findall("Section47")
        for section in sections:
            section_dict = {
                "LAchildID": self.LAchildID,
                "CINdetailsID": self.CINdetailsID,
            }
            section_dict = get_values(elements, section_dict, section)
            sections_list.append(section_dict)

        sections_df = pd.DataFrame(sections_list)
        self.Section47 = pd.concat([self.Section47, sections_df], ignore_index=True)

    # CINdetails and CPPID needed
    def create_ChildProtectionPlans(self, cin_detail):
        """
        Populates the ChildProtectionPlans table. Multiple ChildProtectionPlans blocks can exist in one CINdetails block.

        :param xml child: 'child' element from the XML input
        :returns: DataFrame of data for an individual child for the ChildProtectionPlans Table.
        :rtype: DataFrame
        """

        plans_list = []
        columns = self.ChildProtectionPlans.columns
        elements = list(set(columns).difference(set(self.id_cols)))

        # imitate DfE generator where the first counted thing starts from 1.
        self.CPPID = 0

        plans = cin_detail.findall("ChildProtectionPlans")
        for plan in plans:
            self.CPPID += 1
            plan_dict = {
                "LAchildID": self.LAchildID,
                "CINdetailsID": self.CINdetailsID,
                "CPPID": self.CPPID,
            }
            plan_dict = get_values(elements, plan_dict, plan)
            plans_list.append(plan_dict)

            # functions that should use CPPID before it is incremented
            self.create_Reviews(plan)

        plans_df = pd.DataFrame(plans_list)
        self.ChildProtectionPlans = pd.concat(
            [self.ChildProtectionPlans, plans_df], ignore_index=True
        )

    def create_Reviews(self, plan):
        """
        Populates the ChildIdentifiers table. Multiple Reviews blocks can exist in one Reviews block.

        :param xml child: 'child' element from the XML input
        :returns: DataFrame of data for an individual child for the Reviews Table.
        :rtype: DataFrame
        """

        reviews_list = []
        columns = self.Reviews.columns
        elements = list(set(columns).difference(set(self.id_cols)))

        reviews = plan.findall("Reviews[CPPreviewDate]")
        for review in reviews:
            review_dict = {
                "LAchildID": self.LAchildID,
                "CINdetailsID": self.CINdetailsID,
                "CPPID": self.CPPID,
            }
            review_dict = get_values(elements, review_dict, review)

            reviews_list.append(review_dict)

        reviews_df = pd.DataFrame(reviews_list)
        self.Reviews = pd.concat([self.Reviews, reviews_df], ignore_index=True)


"""
Sidenote: Fields absent from the fake_CIN_data.xml
- Assessments
- CINPlanDates
- Section47
- ChildProtectionPlans
- Reviews
"""
