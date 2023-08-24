from cin_validator.rules.ruleset_utils import get_year_ruleset


def test_ruleset_complete():
    registry = get_year_ruleset("2023")
    # check that there are 107 rules in the 2022/2023 version of CIN rules.
    assert len(registry) == 105
