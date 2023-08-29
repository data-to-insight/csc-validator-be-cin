from pathlib import Path

from cin_validator.rules.ruleset_utils import extract_validator_functions

files = Path(__file__).parent.glob("*.py")
registry = extract_validator_functions(files)

__all__ = ["registry"]
