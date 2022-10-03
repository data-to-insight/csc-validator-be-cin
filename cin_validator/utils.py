import pandas as pd

def make_census_period(collection_year):

    previous_year = str(int(collection_year) - 1)
    collection_start = pd.to_datetime(f'01/04/{previous_year}', format="%d/%m/%Y")
    
    collection_end = pd.to_datetime(f'31/03/{collection_year}', format="%d/%m/%Y")

    return collection_start, collection_end