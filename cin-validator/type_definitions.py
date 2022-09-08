import pandas as pd
from typing import NamedTuple


class dfs(NamedTuple):
    """Data structure that holds the CIN data which the rules need to run on."""

    Header: pd.DataFrame
    ChildIdentifiers: pd.DataFrame
    ChildCharacteristics: pd.DataFrame
    Disabilities: pd.DataFrame
    CINdetails: pd.DataFrame
    Assessments: pd.DataFrame
    CINplanDates: pd.DataFrame
    Section47: pd.DataFrame
    ChildProtectionPlans: pd.DataFrame
    Reviews: pd.DataFrame


class RuleDefinition(NamedTuple):
    """Data structure that holds metadata about rule"""

    code: int
    type: str  # how do I specify the list of accepted values
    module: str  # how do I specify the list of accepted values
    description: str
    affected_fields: str  # why isn't this failing? are these types really enforced?


class PointLocator(NamedTuple):
    """All points in the data that trigger a rule can be uniquely identified by
    - the table name,
    - column name,
    - and index value
    """

    dataframe: pd.DataFrame
    column: str
    index: int
