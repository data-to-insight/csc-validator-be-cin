import pytest

from cin_validator.rule_engine import registry, rule_definition


def test_register_rules():
    registry._registry.clear()

    @rule_definition(
        code=8500,
        rule_type="error",
        module="Child Identifiers",
        message="LA Child ID missing",
        affected_fields=["LAchildID"],
    )
    def validate_8500():
        pass

    @rule_definition(
        code=8501,
        rule_type="error",
        module="Child Identifiers",
        message="LA Child ID missing",
        affected_fields=["LAchildID"],
    )
    def validate_8501():
        pass

    assert len(registry) == 2


def test_register_duplicate_code_raises_error():
    registry._registry.clear()

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

        @rule_definition(
            code=8500,
            rule_type="error",
            module="Child Identifiers",
            message="LA Child ID missing",
            affected_fields=["LAchildID"],
        )
        def validate_8501():
            pass
