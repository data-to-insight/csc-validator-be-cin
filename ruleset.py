from cin_validator.rule_engine import registry

ruleset_updates = []

import cin_validator.rules.cin2022_23

cin22_23 = registry.to_dict()
ruleset_updates.append(([], cin22_23))
registry.reset()

import cin_validator.rules.cin_2023_24

cin23_24 = registry.to_dict()
ruleset_updates.append(([], cin23_24))
registry.reset()

combined_ruleset = ruleset_updates[0][1]
for tup in ruleset_updates[1:]:
    # delete specified rules as specified by their rules codes.
    for deleted_rule in tup[0]:
        del combined_ruleset[deleted_rule]
    # incorporate rule updates and new rules.
    combined_ruleset = combined_ruleset | tup[1]

registry.add_ruleset(combined_ruleset)
