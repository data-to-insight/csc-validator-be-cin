def test_ruleset_complete():
    import cin_validator.rules.cin2022_23
    from cin_validator.rule_engine import registry

    assert len(registry) == 107
