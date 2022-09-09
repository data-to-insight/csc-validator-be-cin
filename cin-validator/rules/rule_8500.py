# TODO make this work on poetry run pytest

import pandas as pd

from type_definitions import dfs, RuleDefinition, PointLocator
from utils import FailedPoints

# can all the files run on this same instance of the function?
# if so, the results can be accumulated at once in one function.
fp = FailedPoints()


def validate_8500():
    rule = RuleDefinition(  # this looks better as a @ decorator
        code=8500,
        type="error",
        module="Child Identifiers",
        description="LA Child ID missing",
        affected_fields=["LAchildID"],
    )

    def _validate(dfs):
        table = dfs.ChildIdentifiers
        # select all the locations where the child ID is absent
        failing_indices = table[table["LAchildID"].isna()].index.to_list()
        failing_points = [
            PointLocator(table, "LAchildID", ind) for ind in failing_indices
        ]  # do this for all columns concerned.
        # return failing_points
        fp.push(failing_points=failing_points)

    return rule, _validate


def test_validate_8500():
    ChildIdentifiers = pd.DataFrame(
        [
            {"LAchildID": "1234"},
            {"LAchildID": pd.NA},
            {"LAchildID": pd.NA},
        ]
    )
    dfs.ChildIdentifiers = ChildIdentifiers
    error_defn, error_func = validate_8500()
    error_func(dfs)  # dfs is short for DataFrames
    print(
        fp.failed_points
        == [
            PointLocator(dfs.ChildIdentifiers, "LAchildID", 1),
            PointLocator(dfs.ChildIdentifiers, "LAchildID", 2),
        ]
    )
    # assert fp.failed_points == [PointLocator(dfs.ChildIdentifiers, 'LAchildID', 1), PointLocator(dfs.ChildIdentifiers, 'LAchildID', 2)]


test_validate_8500()
