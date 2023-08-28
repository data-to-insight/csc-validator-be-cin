from typing import Callable

from cin_validator.rule_engine import RuleContext, RuleDefinition


def run_rule(rule_func: RuleDefinition, datasets: dict) -> RuleContext:
    ctx = RuleContext(rule_func.__rule_def__)
    rule_func(datasets, ctx)
    return ctx
