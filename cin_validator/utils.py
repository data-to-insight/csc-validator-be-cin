from copy import deepcopy

import pandas as pd


def get_values(xml_elements, table_dict, xml_block):
    for element in xml_elements:
        try:
            table_dict[element] = xml_block.find(element).text
        except:
            table_dict[element] = pd.NA
    return table_dict


def make_date(date_input):
    """Allows Ymd or dmY date inputs, used for make_cencus_period.
    Important for test_validate functions"""
    date = pd.to_datetime(date_input, format="%Y/%m/%d", errors="coerce")
    if pd.isna(date):
        date = pd.to_datetime(date_input, format="%d/%m/%Y", errors="coerce")
    return date


def make_census_period(reference_date):
    """Generates the census period.
    input [pd.Series]: ReferenceDate
        column selected from Header DataFrame.
    output [Tuple]: collection_start, collection_end
        datetime objects where collection end equals reference date
        and collection start is April 1st of the previous year"""

    # reference_date is a pandas series. Get it's value as a string by indexing the series' values array.
    reference_date = reference_date.values[0]

    #  Try/except to allow for different datetime formats

    # the ReferenceDate value is always the collection_end date
    collection_end = make_date(reference_date)

    # the collection start is the 1st of April of the previous year.
    collection_start = (
        make_date(reference_date) - pd.DateOffset(years=1) + pd.DateOffset(days=1)
    )

    return collection_start, collection_end


def create_issue_locs(issues):
    """
    input: NamedTuple-like object with fields
            - table
            - columns
            - row_df
    output: DataFrame with fields
            - ERROR_ID
            - ROW_ID
            - columns_affected
            - tables_affected
    """
    df_issue_locs = issues.row_df
    df_issue_locs = df_issue_locs.explode("ROW_ID")

    df_issue_locs["columns_affected"] = df_issue_locs["ERROR_ID"].apply(
        lambda x: issues.columns
    )
    df_issue_locs = df_issue_locs.explode("columns_affected")

    df_issue_locs["tables_affected"] = str(issues.table)[9:]

    df_issue_locs.reset_index(inplace=True)
    df_issue_locs.drop("index", axis=1, inplace=True)

    return df_issue_locs


class DataContainerWrapper:
    def __init__(self, value) -> None:
        self.value = value

    def __getitem__(self, name):
        return getattr(self.value, name.name)

    def __copy__(self):
        cls = self.__class__
        result = cls.__new__(cls)
        result.__dict__.update(self.__dict__)
        return result

    def __deepcopy__(self, memo):
        # the memo param is a dictionary that defines the parts of the class the should be shared between copies.
        cls = self.__class__
        result = cls.__new__(cls)
        memo[id(self)] = result
        for k, v in self.__dict__.items():
            setattr(result, k, deepcopy(v, memo))
        return result


def process_date_columns(df):
    for column in df:
        if ("date" in column) | ("Date" in column):
            try:
                df[column] = df[column].apply(pd.to_datetime, format="%d/%m/%Y")
            except:
                df[column] = df[column].apply(pd.to_datetime, format="%Y/%m/%d")
    return df
