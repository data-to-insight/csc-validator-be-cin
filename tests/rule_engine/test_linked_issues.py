from unittest.mock import Mock

from cin_validator.rule_engine import RuleContext


def test_linked_issue():
    rule_context = RuleContext(Mock())

    # The signature of this method does not give any hints as to how to use it, so I'm a bit lost at this stage
    rule_context.push_linked_issues(
        [
            ("TableName", "ColumnName", [1, 2, 3], ["A"]),
        ]
    )

    # Also, not quite sure what I expect here...
    assert rule_context.linked_issues == []
