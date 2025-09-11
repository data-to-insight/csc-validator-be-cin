from cin_validator.rules.ruleset_utils import get_year_ruleset


def test_ruleset_complete():
    registry = get_year_ruleset("2023")
    # check that there are 105 rules in the 2022/2023 version of CIN rules.
    assert len(registry) == 105

    registry = get_year_ruleset("2024")
    # check that the 2023/2024 version of CIN rules pulls in the preceding year's rules.
    assert len(registry) == 105

    registry = get_year_ruleset("2025")
    # check that the 2024/2025 version of CIN rules pulls in the preceding year's rules.
    assert len(registry) == 109

    registry = get_year_ruleset("2026")
    # check that the 2024/2025 version of CIN rules pulls in the preceding year's rules.
    assert len(registry) == 109
