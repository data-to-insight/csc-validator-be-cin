from functools import wraps
from typing import Callable, Iterable

from cin_validator.rule_engine.__api import CINTable, RuleDefinition, RuleType


class __Registry:
    """Used to contain information about validation rules including definition
    to allow iterating through validation rules.
    """

    def __init__(self):
        """Initialises an empty dicitonary to be filled with validation rules and their
        RuleDefinitions.
        """
        self._registry = {}

    def add(self, rd: RuleDefinition):
        """Adds rules to the registry for iterating through and validating.

        :param RuleDefinition-object: Object containing rule definition for every validation rule.
        :returns: Adds rule definition fo rule to registry. Error if the rule code already exists.
        :rtype: RuleDefinition object dictionary entry.
        """
        if rd.code in self._registry:
            raise ValueError(f"Rule with code {rd.code} already exists")
        self._registry[rd.code] = rd

    def get(self, code: int):
        """Extracts code for each validation rule.

        :param int code: The code for a validation rule.
        :returns: Rule code for validation rule.
        :rtype: int
        """
        return self._registry.get(code)

    def __getitem__(self, code: int):
        """Used to return individual rules by code to allow iterating.

        :param int code: The code for a particular validation rule.
        :returns: A RuleDefinition for a particular validation rule, by rule code.
        :rtype: RuleDefinition object.
        """
        return self._registry[code]

    def __len__(self):
        """Provides the length of the number of validation rules.

        :returns: The length of the number of rules in the registry.
        :rtype: int.
        """
        return len(self._registry)

    def __iter__(self):
        """Allows iterating through validation rules by code.

        :returns: Iterable of validation rules.
        :rtype: Iterable.
        """
        return iter(self._registry.values())


registry = __Registry()


def rule_definition(
    code: int,
    module: CINTable,
    rule_type: RuleType = RuleType.ERROR,
    message: str = None,
    affected_fields: Iterable[str] = None,
):
    """Creates the rule definition for validation rules filling out
    the RuleDefinition class.

    :param int code: The rule code for each rule.
    :param RuleType-class rule_type: A RuleType class object containing a string denoting if
        the rule is an error or a query.
    :param CINtable-object module: Contains a string denoting the module/table affected by a
        validation rule.
    :param str affected_fields: The fields/columns affecte dby a validation rule.
    :param str message: The message displayed for each validation rule.
    :returns: RuleDefinition object containing information about validation rules.
    :rtype: RuleDefiniton class object.
    """

    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            return func(*args, **kwargs)

        definition = RuleDefinition(
            code=code,
            func=func,
            rule_type=rule_type,
            module=module,
            message=message,
            affected_fields=affected_fields,
        )
        registry.add(definition)
        wrapper.__rule_def__ = definition
        return wrapper

    return decorator
