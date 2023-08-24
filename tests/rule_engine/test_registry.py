import os
from pathlib import Path

import pytest
from testfixtures import tempdir

from cin_validator.rule_engine import rule_definition
from cin_validator.rules.ruleset_utils import (
    check_duplicate_rules,
    extract_validator_functions,
)


@tempdir()
def test_register_rules(dir):
    file_content = """
    \nfrom cin_validator.rule_engine import rule_definition
    \n@rule_definition(
        code=8500,
        rule_type="error",
        module="Child Identifiers",
        message="LA Child ID missing",
        affected_fields=["LAchildID"],
    )
    \ndef validate_8500():
        pass

    \n@rule_definition(
        code=8501,
        rule_type="error",
        module="Child Identifiers",
        message="LA Child ID missing",
        affected_fields=["LAchildID"],
    )
    \ndef validate_8501():
        pass
    """

    # write the content above into a temporary file
    with open("tests/rule_test_registry.py", "w") as f:
        f.write(file_content)
    path = Path(dir.path) / "tests/rule_test_registry.py"
    registry = extract_validator_functions([path])

    assert len(registry) == 2
    os.remove("tests/rule_test_registry.py")


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
