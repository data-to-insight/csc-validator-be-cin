import pandas as pd


def get_values(xml_elements, table_dict, xml_block):
    for element in xml_elements:
        try:
            table_dict[element] = xml_block.find(element).text
        except:
            table_dict[element] = pd.NA
    return table_dict


def make_census_period(reference_date):
    """Generates the census period.
    input [pd.Series]: ReferenceDate
        column selected from Header DataFrame.
    output [Tuple]: collection_start, collection_end
        datetime objects where collection end equals reference date
        and collection start is April 1st of the previous year"""

    # reference_date is a pandas series. Get it's value as a string by indexing the series' values array.
    reference_date = reference_date.values[0]

    # the ReferenceDate value is always the collection_end date
    collection_end = pd.to_datetime(reference_date, format="%d/%m/%Y")
    # the collection start is the 1st of April of the previous year.
    collection_start = (
        pd.to_datetime(reference_date, format="%d/%m/%Y")
        - pd.DateOffset(years=1)
        + pd.DateOffset(days=1)
    )

    return collection_start, collection_end


class DataContainerWrapper:
    def __init__(self, value) -> None:
        self.value = value

    def __getitem__(self, name):
        return getattr(self.value, name.name)
