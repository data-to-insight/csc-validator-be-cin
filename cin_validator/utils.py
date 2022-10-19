import pandas as pd


def get_values(xml_elements, table_dict, xml_block):
    for element in xml_elements:
        try:
            table_dict[element] = xml_block.find(element).text
        except:
            table_dict[element] = pd.NA
    return table_dict


def make_census_period(collection_year):

    previous_year = str(int(collection_year) - 1)
    collection_start = pd.to_datetime(f"01/04/{previous_year}", format="%d/%m/%Y")

    collection_end = pd.to_datetime(f"31/03/{collection_year}", format="%d/%m/%Y")

    return collection_start, collection_end


class DataContainerWrapper:
    def __init__(self, value) -> None:
        self.value = value

    def __getitem__(self, name):
        return getattr(self.value, name.name)
