import json
import re


def extract_rule_code(filepath):
    pattern = r"rule_(\d+)\.py"
    match = re.search(pattern, filepath)
    if match:
        return match.group(1)
    else:
        return None


with open("files_failed.json", "r") as f:
    filepaths = json.load(f)
# filepath = "C:\\Users\\tambe.tabitha\\CIN-validator\\cin_validator\\rules\\cin2022_23\\rule_8940.py"

for filepath in filepaths:
    print(f"+++++++++++++++++++++++++++++++{filepath}+++++++++++++++++++++++++++++++")
    try:
        rule_code = extract_rule_code(filepath)

        with open(filepath, "r") as f:
            file_text = f.read()
            file_text = file_text.replace(f"{rule_code}", f"'{rule_code}'")
        with open(filepath, "w") as f:
            f.write(file_text)
            print(f"updated {rule_code} to '{rule_code}'")
    except Exception as e:
        print(f"Error updating {filepath}: {e}")
        continue
