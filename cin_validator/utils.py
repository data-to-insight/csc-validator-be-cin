from copy import deepcopy

import pandas as pd


def get_values(xml_elements, table_dict, xml_block):
    """Iterates through the input XML to extract values from XML input data for validation.
    Called in the ingress to create each table.

     :param list xml_elements: Contains elements of the collection to add to dictionary.
     :param dictionary table_dict: Dictionary containing columns of each table to get values for.
     :param DataFrame xml_block: Contains the name of the block to search the XML to make each table.
        (Tambe these are DataFrames in ingress but are they DataFrames here?)
     :returns: table_dict with XML elements where they exist, and pd.NA where they do not.
     :rtype: Dictionary
    """
    for element in xml_elements:
        try:
            table_dict[element] = xml_block.find(element).text
        except:
            table_dict[element] = pd.NA
    return table_dict


def make_date(date_input):
    """Allows Ymd or dmY date inputs, used for make_census_period.
    Important for test_validate functions.

    :param str date_input: Contains the data data to be converted to pd.datetime
        object as a string.
    :reutrns: Date data input as pd.datetime object.
    :rtype: pd.datetime object.
    """
    date = pd.to_datetime(date_input, format="%Y/%m/%d", errors="coerce")
    if pd.isna(date):
        date = pd.to_datetime(date_input, format="%d/%m/%Y", errors="coerce")
    return date


def make_census_period(reference_date):
    """Generates the census period with variables for each of the first and last
    day of the census period. Thesze are April first of the previous year and the reference date.

    :param DataFrame reference_date: A DataFrame containing data with the reference date of
        the data being validated, selected from the Header DataFrame ReferenceDate column.
    :returns: Dates of collection start and colleciton end as pd.datetime variables collection_start and
        collection_end as a tuple.
    :rtype: Tuple
    """

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
    """Used when validating rules to provide locations of issues/errors data to locate
    module/column/row/cells affected by the issues.

    :param NamedTuple-like-object issues: An object containing the fields for table, columns, and
        row_df for issues found when validating data.
    :returns: DataFrame with fields for ERROR_ID, ROW_ID, columns_affected, and tables_affected for
        issues found in validation.
    :rtype: DataFrame
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
    """Takes a DataFrame in and converts all columns with Date or date in the
    title to pd.datetime objects. Used before validating rules to allow
    comparisons between dates and datetime methods.

    :param DataFrame df: DataFrame containing data to be validated.
    :returns: DataFrame with date columns as pd.datetime objects.
    :rtype: DataFrame
    """
    for column in df:
        if ("date" in column) | ("Date" in column):
            try:
                df[column] = pd.to_datetime(
                    df[column], format="%d/%m/%Y", errors="coerce"
                )
            except:
                df[column] = pd.to_datetime(
                    df[column], format="%Y/%m/%d", errors="coerce"
                )
    return df
