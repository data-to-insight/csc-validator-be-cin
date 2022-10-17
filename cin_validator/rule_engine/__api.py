import importlib
from dataclasses import dataclass
from enum import Enum
from typing import Callable, Iterable, NamedTuple


class CINTable(Enum):
    Header = Enum(
        "Header",
        [
            "Collection",
            "Year",
            "ReferenceDate",
            "SourceLevel",
            "LEA",
            "SoftwareCode",
            "Release",
            "SerialNo",
            "DateTime",
        ],
    )
    ChildIdentifiers = Enum(
        "ChildIdentifiers",
        [
            "LAchildID",
            "UPN",
            "FormerUPN",
            "UPNunknown",
            "PersonBirthDate",
            "ExpectedPersonBirthDate",
            "GenderCurrent",
            "PersonDeathDate",
        ],
    )
    ChildCharacteristics = Enum(
        "ChildCharacteristics",
        [
            "LAchildID",
            "Ethnicity",
        ],
    )
    Disabilities = Enum("Disabilities", ["LAchildID", "Disability"])
    CINdetails = Enum(
        "CINdetails",
        [
            "LAchildID",
            "CINdetailsID",
            "CINreferralDate",
            "ReferralSource",
            "PrimaryNeedCode",
            "CINclosureDate",
            "ReasonForClosure",
            "DateOfInitialCPC",
            "ReferralNFA",
        ],
    )
    Assessments = Enum(
        "Assessments",
        [
            "LAchildID",
            "CINdetailsID",
            "AssessmentActualStartDate",
            "AssessmentInternalReviewDate",
            "AssessmentAuthorisationDate",
            "AssessmentFactors",
        ],
    )
    CINplanDates = Enum(
        "CINplanDates",
        ["LAchildID", "CINdetailsID", "CINPlanStartDate", "CINPlanEndDate"],
    )
    Section47 = Enum(
        "Section47",
        [
            "LAchildID",
            "CINdetailsID",
            "S47ActualStartDate",
            "InitialCPCtarget",
            "DateOfInitialCPC",
            "ICPCnotRequired",
        ],
    )
    ChildProtectionPlans = Enum(
        "ChildProtectionPlans",
        [
            "LAchildID",
            "CINdetailsID",
            "CPPID",
            "CPPstartDate",
            "CPPendDate",
            "InitialCategoryOfAbuse",
            "LatestCategoryOfAbuse",
            "NumberOfPreviousCPP",
        ],
    )
    Reviews = Enum("Reviews", ["LAchildID", "CINdetailsID", "CPPID", "CPPreviewDate"])

    def __getattr__(self, item):
        if not item.startswith("_"):
            try:
                return self.value[item].name
            except KeyError as kerr:
                raise AttributeError(f"Table {self.name} has no field {item}") from kerr
        else:
            return super().__getattr__(item)


class RuleType(Enum):
    ERROR = "Error"
    QUERY = "Query"


@dataclass(frozen=True, eq=True)
class RuleDefinition:
    code: int
    func: Callable
    rule_type: RuleType = RuleType.ERROR
    module: CINTable = None
    affected_fields: Iterable[str] = None
    message: str = None

    @property
    def code_module(self):
        return importlib.import_module(self.func.__module__)
