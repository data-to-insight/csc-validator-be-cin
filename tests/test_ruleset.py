def test_ruleset_complete():
    # registry is filled with whatever ruleset is imported.
    # If multiple rulesets are imported, the registry will contain the sum of them all.
    import cin_validator.rules.cin2022_23
    from cin_validator.rule_engine import registry

    # check that there are 107 rules in the 2022/2023 version of CIN rules.
    assert len(registry) == 107
