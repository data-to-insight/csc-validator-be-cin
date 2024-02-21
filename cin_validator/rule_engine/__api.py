import importlib
from dataclasses import dataclass
from enum import Enum
from typing import Callable, Iterable, Optional


class CINTable(Enum):
    """
    An enumeration class (https://docs.python.org/3/library/enum.html).
    Used in validation to select CIN data modules/tables and fields/columns
    and assign them to variables. For practical reasons, this is done to
    ensure consistent spelling.
    """

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
            "Sex",
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
            "AssessmentID",
            "AssessmentActualStartDate",
            "AssessmentInternalReviewDate",
            "AssessmentAuthorisationDate",
            "AssessmentFactors",
        ],
    )
    AssessmentFactorsList = Enum(
        "AssessmentFactorsList",
        [
            "LAchildID",
            "CINdetailsID",
            "AssessmentID",
            "AssessmentFactor",
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
        """
        Used to get attributes within the CINtable class. Practically used to define
        fields/column variables from within tables for use in validation rules.

        :param variable item: The name of a module and field to be used for
            a validation rule.
        :returns: A variable containing a field/column for validation, or an error
            (generally on misspelling).
        :rtype: Variable, error.
        """

        if not item.startswith("_"):
            try:
                return self.value[item].name
            except KeyError as kerr:
                raise AttributeError(f"Table {self.name} has no field {item}") from kerr
        else:
            return super().__getattr__(item)


class RuleType(Enum):
    """
    An enumeration type class that defines available rule types.
    Used to assign 'Error' or 'Query' to each rule in validation.
    """

    ERROR = "Error"
    QUERY = "Query"


@dataclass(frozen=True, eq=True)
class RuleDefinition:
    """
    A dataclass type class used in each validation to assign information about
    each validation rule to the rule.

    :param int code: The rule code for each rule.
    :param function func: Used to import the validation rule function.
    :param RuleType-class rule_type: A RuleType class object accepts a string denoting if
        the rule is an error or a query.
    :param CINtable-object module: Accepts a string denoting the module/table affected by a
        validation rule.
    :param str affected_fields: The fields/columns affected by a validation rule.
    :param str message: The message to be displayed if rule is flagged.
    :returns: RuleDefinition object containing information about validation rules.
    :rtype: dataclass object.
    """

    code: str
    func: Callable
    rule_type: RuleType = RuleType.ERROR
    module: Optional[CINTable] = None
    affected_fields: Optional[Iterable[str]] = None
    message: Optional[str] = None


@dataclass(eq=True)
class YearConfig:
    deleted: list[str]
    added_or_modified: dict[str, RuleDefinition]
