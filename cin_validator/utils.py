import pandas as pd


def get_values(xml_elements, table_dict, xml_block):
    for element in xml_elements:
        try:
            table_dict[element] = xml_block.find(element).text
        except:
            table_dict[element] = pd.NA
    return table_dict


def make_date(date):
    try:
        date = pd.to_datetime(date, format="%Y/%m/%d", errors="coerce")
    except:
        date = pd.to_datetime(date, format="%d/%m/%Y", errors="coerce")

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


class DataContainerWrapper:
    def __init__(self, value) -> None:
        self.value = value

    def __getitem__(self, name):
        return getattr(self.value, name.name)


class ErrorReport:
    """Class containing rules, number of errors per rule, and locations
    per rule."""

    def __init__(self, codes: int, number: int, locations, message: str):
        self.codes = codes
        self.number = number
        self.locations = locations
        self.message = message
