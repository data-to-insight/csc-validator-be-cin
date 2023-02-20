from pathlib import Path

__all__ = [
    p.stem for p in Path(__file__).parent.glob("*.py") if p.stem != "__init__"
].append(
    [
        p.stem
        for p in Path("cin_validator/rules/cin2022_23").parent.glob("*.py")
        if p.stem
        in [
            "__init__",
            "rule_4009",
            "rule_8525",
            "rule_8555",
            "rule_8569",
            "rule_8863",
            "rule_8873",
            "rule_8897",
        ]
    ]
)

from . import *