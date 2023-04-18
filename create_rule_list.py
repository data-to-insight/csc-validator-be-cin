import json

import pandas as pd

from cin_validator.ruleset import create_registry

registry = create_registry(ruleset="cin2022_23")
all_rules = []
for rule in registry:
    all_rules.append(
        {"value": str(rule.code), "label": str(rule.code) + " - " + str(rule.message)}
    )

all_rules = pd.DataFrame(all_rules)
all_rules = all_rules.to_json(orient="records")

with open("write_all_rules.json", "w") as f:
    json.dump(all_rules, f)
