# import pytest
import pandas as pd
from pandas.api.types import is_datetime64_any_dtype as is_datetime

from cin_validator.utils import process_date_columns


def test_date_process_function():
    df = pd.DataFrame(
        [
            {
                "Aniversaries": [
                    "2022/04/25",
                    "2022/03/01",
                    "2022/12/25",
                    "2021/04/27",
                    "2021/11/21",
                    "2021/08/20",
                    "2020/04/17",
                    "1999/01/30",
                ],
            },
            {
                "Dates": [
                    "2022/04/25",
                    "2022/03/01",
                    "2022/12/25",
                    "2021/04/27",
                    "2021/11/21",
                    "2021/08/20",
                    "2020/04/17",
                    "1999/01/30",
                ],
            },
            {
                "dates": [
                    "2022/04/25",
                    "2022/03/01",
                    "2022/12/25",
                    "2021/04/27",
                    "2021/11/21",
                    "2021/08/20",
                    "2020/04/17",
                    "1999/01/30",
                ],
            },
        ]
    )
    print(df.info())
    df = process_date_columns(df)
    print(df.info())
    assert df["Aniversaries"].dtype == object
    assert is_datetime(df["dates"])
    assert is_datetime(df["Dates"])
