import json

import pandas as pd

import cin_validator.rules.cin2022_23
from cin_validator.rule_engine import registry

all_rules = []
for rule in registry:
    all_rules.append(
        {"value": str(rule.code), "label": str(rule.code) + " - " + str(rule.message)}
    )

all_rules = pd.DataFrame(all_rules)
all_rules = all_rules.to_dict(orient="records")

with open("write_all_rules.json", "w") as f:
    json.dump(all_rules, f)
