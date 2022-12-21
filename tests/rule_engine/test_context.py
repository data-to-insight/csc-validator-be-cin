from unittest.mock import Mock

import pandas as pd

from cin_validator.rule_engine import IssueLocator, RuleContext


def test_issues():
    rule_context = RuleContext(Mock())
    rule_context.push_issue("table_name", "column_name", [4, 7, 8])

    assert list(rule_context.issues) == [
        IssueLocator("table_name", "column_name", 4),
        IssueLocator("table_name", "column_name", 7),
        IssueLocator("table_name", "column_name", 8),
    ]


def test_type1():
    """rules that involve columns in the same table which were not joined by merge."""
    rule_context = RuleContext(Mock())
    df_issues = pd.DataFrame(
        [
            {"ERROR_ID": (2, 8, 5), "ROW_ID": [23, 24]},
            {"ERROR_ID": (3, 7, 2), "ROW_ID": [9]},
        ]
    )
    rule_context.push_type_1("table_name", ["column1", "column2"], df_issues)

    issues = rule_context.type1_issues
    assert issues.table == "table_name"
    assert issues.columns == ["column1", "column2"]
    assert issues.row_df.equals(df_issues)


def test_type_one():
    """Expands type1_issues object into a dataframe."""
    rule_context = RuleContext(Mock())
    df_issues = pd.DataFrame(
        [
            {"ERROR_ID": (2, 8, 5), "ROW_ID": [23, 24]},
            {"ERROR_ID": (3, 7, 2), "ROW_ID": [9]},
        ]
    )
    # check type_one_issues value for rules that don't push to it. i.e non-type1 rules.
    assert rule_context.type_one_issues == []

    # check behaviour for rules that run push_type1
    rule_context.push_type_1("CINTable.table_name", ["column1", "column2"], df_issues)
    issues = rule_context.type_one_issues

    # check that the dataframe columns are generated as expected when push_type1 is used.
    assert (
        issues["ERROR_ID"]
        == pd.Series(
            [
                (2, 8, 5),
                (2, 8, 5),
                (2, 8, 5),
                (2, 8, 5),
                (3, 7, 2),
                (3, 7, 2),
            ]
        )
    ).all()
    assert (issues["ROW_ID"] == pd.Series([23, 23, 24, 24, 9, 9])).all()
    assert (
        issues["columns_affected"]
        == pd.Series(["column1", "column2", "column1", "column2", "column1", "column2"])
    ).all()

    assert issues["tables_affected"].unique() == ["table_name"]


def test_type2():
    """
    Stores linked error locations for rules that involve mutiple columns in multiple tables
    """
    rule_context = RuleContext(Mock())
    df_issues_1 = pd.DataFrame(
        [
            {"ERROR_ID": (2, 8, 5), "ROW_ID": [23, 24]},
            {"ERROR_ID": (3, 7, 2), "ROW_ID": [18]},
        ]
    )
    df_issues_2 = pd.DataFrame(
        [
            {"ERROR_ID": (3, 7, 2), "ROW_ID": [9]},
        ]
    )
    df_issues_3 = pd.DataFrame(
        [
            {"ERROR_ID": (2, 8, 5), "ROW_ID": [0, 1]},
            {"ERROR_ID": (3, 7, 2), "ROW_ID": [9]},
        ]
    )
    rule_context.push_type_2("table_one", ["column1", "column2"], df_issues_1)
    rule_context.push_type_2("table_two", ["column3"], df_issues_2)
    rule_context.push_type_2("table_three", ["column4", "column5"], df_issues_3)

    issues_list = rule_context.type2_issues
    assert isinstance(issues_list, list)
    assert len(issues_list) == 3
    # choose a table and check it's content
    issues = issues_list[2]
    assert issues.table == "table_three"
    assert issues.columns == ["column4", "column5"]
    assert issues.row_df.equals(df_issues_3)


def test_type_two():
    """expands type2_issues object into a dataframe."""
    rule_context = RuleContext(Mock())
    df_issues_1 = pd.DataFrame(
        [
            {"ERROR_ID": (3, 7, 2), "ROW_ID": [18, 24]},
        ]
    )
    df_issues_2 = pd.DataFrame(
        [
            {"ERROR_ID": (3, 7, 2), "ROW_ID": [9]},
        ]
    )

    assert rule_context.type_two_issues == []

    rule_context.push_type_2("CINTable.table_one", ["column1", "column2"], df_issues_1)
    rule_context.push_type_2("CINTable.table_two", ["column3"], df_issues_2)

    issues = rule_context.type_two_issues
    assert (
        issues["tables_affected"]
        == pd.Series(
            [
                "table_one",
                "table_one",
                "table_one",
                "table_one",
                "table_two",
            ]
        )
    ).all()
    assert (
        issues["ERROR_ID"]
        == pd.Series(
            [
                (3, 7, 2),
                (3, 7, 2),
                (3, 7, 2),
                (3, 7, 2),
                (3, 7, 2),
            ]
        )
    ).all()
    assert (issues["ROW_ID"] == pd.Series([18, 18, 24, 24, 9])).all()
    assert (
        issues["columns_affected"]
        == pd.Series(
            [
                "column1",
                "column2",
                "column1",
                "column2",
                "column3",
            ]
        )
    ).all()


def test_type3():
    """Rules that check relationships within a group."""
    rule_context = RuleContext(Mock())
    df_issues = pd.DataFrame(
        [
            {"ERROR_ID": (2, 8, 5), "ROW_ID": [23, 24]},
            {"ERROR_ID": (3, 7, 2), "ROW_ID": [9]},
        ]
    )
    rule_context.push_type_3("a_table", ["column1", "column2"], df_issues)
    rule_context.push_type_3("table_name", ["column1"], df_issues)

    issues_list = rule_context.type3_issues
    issues = issues_list[1]
    assert issues.table == "table_name"
    assert len(issues.columns) == 1
    assert issues.columns == [
        "column1",
    ]
    assert issues.row_df.equals(df_issues)


def test_type_three():
    """expands type3_issues object into a dataframe."""
    rule_context = RuleContext(Mock())
    df_issues_1 = pd.DataFrame(
        [
            {"ERROR_ID": (3, 7, 2), "ROW_ID": [18, 24]},
        ]
    )
    df_issues_2 = pd.DataFrame(
        [
            {"ERROR_ID": (3, 9, 4), "ROW_ID": [10]},
        ]
    )

    assert rule_context.type_two_issues == []

    rule_context.push_type_3("CINTable.table_one", ["column1", "column2"], df_issues_1)
    rule_context.push_type_3("CINTable.table_two", ["column3"], df_issues_2)

    issues = rule_context.type_three_issues
    assert (
        issues["tables_affected"]
        == pd.Series(
            [
                "table_one",
                "table_one",
                "table_one",
                "table_one",
                "table_two",
            ]
        )
    ).all()
    assert (
        issues["ERROR_ID"]
        == pd.Series(
            [
                (3, 7, 2),
                (3, 7, 2),
                (3, 7, 2),
                (3, 7, 2),
                (3, 9, 4),
            ]
        )
    ).all()
    assert (issues["ROW_ID"] == pd.Series([18, 18, 24, 24, 10])).all()
    assert (
        issues["columns_affected"]
        == pd.Series(
            [
                "column1",
                "column2",
                "column1",
                "column2",
                "column3",
            ]
        )
    ).all()


def test_la_level():
    """Rules that check relationships across the whole local authority"""
    rule_context = RuleContext(Mock())
    rule_context.code = "2000"
    rule_context.description = "return-level validation rule"
    check = pd.DataFrame([1])
    if len(check):
        rule_context.push_la_level(rule_context.code, rule_context.description)
    else:
        pass

    issues = rule_context.la_issues
    assert issues == ("2000", "return-level validation rule")
