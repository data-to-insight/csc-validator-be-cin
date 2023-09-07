from functools import wraps
from typing import Callable, Iterable, Optional

from cin_validator.rule_engine.__api import CINTable, RuleDefinition, RuleType


def rule_definition(
    code: str,
    module: CINTable,
    rule_type: RuleType = RuleType.ERROR,
    message: Optional[str] = None,
    affected_fields: Optional[Iterable] = None,
):
    """
    Creates the rule definition for validation rules using RuleDefinition class as a template.

    :param int code: The rule code for each rule.
    :param RuleType-class rule_type: object denoting if the rule is an error or a query.
    :param CINtable-object module: string denoting the module/table affected by a validation rule.
    :param str affected_fields: The fields/columns affected by a validation rule.
    :param str message: The message displayed for each validation rule.
    :returns: RuleDefinition object containing information about validation rules.
    :rtype: RuleDefiniton class object.
    """

    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            return func(*args, **kwargs)

        definition = RuleDefinition(
            code=str(code),
            func=func,
            rule_type=rule_type,
            module=module,
            message=message,
            affected_fields=affected_fields,
        )
        wrapper.__rule_def__ = definition
        return wrapper

    return decorator
