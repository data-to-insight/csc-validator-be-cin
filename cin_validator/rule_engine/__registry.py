from functools import wraps
from typing import Callable, Iterable

from cin_validator.rule_engine.__api import RuleDefinition, RuleType, CINTable


class __Registry:
    def __init__(self):
        self._registry = {}

    def add(self, rd: RuleDefinition):
        if rd.code in self._registry:
            raise ValueError(f"Rule with code {rd.code} already exists")
        self._registry[rd.code] = rd

    def get(self, code: int):
        return self._registry.get(code)

    def __getitem__(self, code: int):
        return self._registry[code]

    def __len__(self):
        return len(self._registry)

    def __iter__(self):
        return iter(self._registry.values())


registry = __Registry()


def rule_definition(
    code: int,
    module: CINTable,
    rule_type: RuleType = RuleType.ERROR,
    description: str = None,
    affected_fields: Iterable[str] = None,
):
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            return func(*args, **kwargs)

        definition = RuleDefinition(
            code=code,
            func=func,
            rule_type=rule_type,
            module=module,
            description=description,
            affected_fields=affected_fields,
        )
        registry.add(definition)
        wrapper.__rule_def__ = definition
        return wrapper

    return decorator
