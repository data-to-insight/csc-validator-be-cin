import numpy as np
import pandas as pd

from cin_validator.england_holidates import england_holidates


def get_values(xml_elements, table_dict: dict, xml_block):
    """
    Iterates through the input XML to extract values from XML input data for validation.
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


def make_date(date_input: str):
    """
    Allows Ymd or dmY date inputs, used for make_census_period.
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


def make_census_period(reference_date: pd.Series):
    """
    Generates the census period with variables for each of the first and last
    day of the census period. Thesze are April first of the previous year and the reference date.

    :param DataFrame reference_date: A DataFrame containing data with the reference date of
        the data being validated, selected from the Header DataFrame ReferenceDate column.
    :returns: Dates of collection start and colleciton end as pd.datetime variables collection_start and
        collection_end as a tuple.
    :rtype: Tuple
    """

    # reference_date is a pandas series. Get it's value as a string by indexing the series' values array.
    reference_date = reference_date.values[0]

    # the ReferenceDate value is always the collection_end date
    collection_end = make_date(reference_date)

    # the collection start is the 1st of April of the previous year but dates from the day of the previous collection move to the next collection.
    # e.g. in the 22-23 collection, 2022-03-31 is an allowed date according to the test data.
    collection_start = make_date(reference_date) - pd.DateOffset(years=1)

    return collection_start, collection_end


def create_issue_locs(issues):
    """
    Reverses grouping of issue rows, creating a DataFrame where each row contains a single issue location.

    :param NamedTuple-like-object issues: An object containing the fields for table, columns, and
        row_df for issues found when validating data.
    :returns: DataFrame with fields for ERROR_ID, ROW_ID, columns_affected, and tables_affected for
        issues found in validation.
    :rtype: DataFrame
    """

    # expand the row_id groups such that row_id value exists per row instead of a list
    df_issue_locs = issues.row_df
    df_issue_locs = df_issue_locs.explode("ROW_ID")

    # map every row_id to its respective columns_affected list and expand that list
    df_issue_locs["columns_affected"] = df_issue_locs["ERROR_ID"].apply(
        lambda x: issues.columns
    )
    df_issue_locs = df_issue_locs.explode("columns_affected")

    # all locations from a NamedTuple object will have the same singular value of tables_affected.
    df_issue_locs["tables_affected"] = str(issues.table)[9:]

    # now a one-to-one relationship exists across table-column-row
    df_issue_locs.reset_index(inplace=True)
    df_issue_locs.drop("index", axis=1, inplace=True)

    return df_issue_locs


def process_date_columns(df: pd.DataFrame):
    """
    Takes a DataFrame in and converts all columns with Date or date in the
    title to pd.datetime objects. Used before validating rules to allow
    comparisons between dates and datetime methods.

    :param DataFrame df: DataFrame containing data to be validated.
    :returns: DataFrame with date columns formatted to pd.datetime objects.
    :rtype: DataFrame
    """

    for column in df:
        if "date" in column.lower():
            # pd.to_datetime is intelligent. It can deal with unforseen date formats
            df[column] = pd.to_datetime(df[column], errors="coerce")
    return df


def create_holidays_array():
    """
    :return numpy-object _: business day calendar object that considers the bank holiday calendar of England and Wales
    """
    return np.busdaycalendar(holidays=england_holidates)


def england_working_days(num_days: int):
    """
    This function implements a date offset based on a holiday calendar.
    :param int num_days: number of days to offset by
    :return pd.DateOffset-obj _: date offset
    """

    holiday_calendar = create_holidays_array()

    # pd.offsets.CustomBusinessDay doesn't seem to include the end date so offset by 1 so that it does.
    return pd.offsets.CustomBusinessDay(n=num_days - 1, calendar=holiday_calendar)
