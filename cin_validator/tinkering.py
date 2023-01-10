import pandas as pd
from utils import make_date
frame = pd.DataFrame(
    [
            {
                "LAchildID": "child1",  # 0 Pass
                "CPPstartDate": "26/05/2000",
                "CPPendDate": "26/10/2000",
                "CPPID": "cinID1",
            },
            {
                "LAchildID": "child1",  # 1 Fail
                "CPPstartDate": "26/08/2000",
                "CPPendDate": "26/12/2000",
                "CPPID": "cinID12",
            },
            {
                "LAchildID": "child2",  # 2 Pass
                "CPPstartDate": "26/05/2000",
                "CPPendDate": "25/10/2000",
                "CPPID": "cinID2",
            },
            {
                "LAchildID": "child2",  # 3 Pass
                "CPPstartDate": "26/10/2000",
                "CPPendDate": "26/12/2000",
                "CPPID": "cinID22",
            },
            {
                "LAchildID": "child3",  # 4 Pass
                "CPPstartDate": "26/05/2000",
                "CPPendDate": pd.NA,
                "CPPID": "cinID3",
            },
            {
                "LAchildID": "child3",  # 5 Fail
                "CPPstartDate": "26/08/2000",
                "CPPendDate": "26/10/2000",
                "CPPID": "cinID32",
            },
            {
                "LAchildID": "child4",  # 6 Pass
                "CPPstartDate": "26/10/2000",
                "CPPendDate": "31/03/2001",
                "CPPID": "cinID4",
            },
            {
                "LAchildID": "child4",  # 7 Fail
                "CPPstartDate": "31/03/2001",
                "CPPendDate": pd.NA,
                "CPPID": "cinID42",
            },
        ]
        )

frame2 = pd.DataFrame(
    [
            {
                "LAchildID": "child1",  # 0 Pass
                "CPPstart": "26/05/2000",
                "CPPend": "26/10/2000",
                "CPPID": "cinID1",
            },
    ]
    )


def process_date_columns(df):
    for column in df:
        if ('date' in column) | ('Date' in column):
            try:
                df[column] = df[column].apply(pd.to_datetime, format = '%d/%m/%Y')
            except:
                df[column] = df[column].apply(pd.to_datetime, format = '%Y/%m%d')
    return df

process_date_columns(frame)
print(frame)
