from enum import Enum
from typing import NamedTuple

import pandas as pd


class CINTables(Enum):
    HEADER = "Header"
    CHILD_IDENTIFIERS = "ChildIdentifiers"


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


class PointLocator(NamedTuple):
    """All points in the data that trigger a rule can be uniquely identified by
    - the table name,
    - column name,
    - and index value
    """

    dataframe: pd.DataFrame
    column: str
    index: int
