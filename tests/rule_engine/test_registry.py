import pytest

from cin_validator.rule_engine import rule_definition
from cin_validator.rules.ruleset_utils import check_duplicate_rules


def test_register_duplicate_code_raises_error():
    with pytest.raises(ValueError):

        @rule_definition(
            code=8500,
            rule_type="error",
            module="Child Identifiers",
            message="LA Child ID missing",
            affected_fields=["LAchildID"],
        )
        def validate_8500():
            pass

        funcs_so_far = {"8500": validate_8500}

        @rule_definition(
            code=8500,
            rule_type="error",
            module="Child Identifiers",
            message="LA Child ID missing",
            affected_fields=["LAchildID"],
        )
        def validate_8501():
            pass

        new_funcs = {"8500": validate_8501}
        check_duplicate_rules(new_funcs, funcs_so_far)
