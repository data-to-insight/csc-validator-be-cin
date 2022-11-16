import pandas as pd


def get_values(xml_elements, table_dict, xml_block):
    for element in xml_elements:
        try:
            table_dict[element] = xml_block.find(element).text
        except:
            table_dict[element] = pd.NA
    return table_dict


def make_date(date_input):
    """Allows Ymd or dmY date inputs, used for make_cancus_period.
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


def process_issues(rule, ctx, individual_error_df):
    error_dict = {"code": rule.code, "number": len(list(ctx.issues)), "type": 0}
    for i in range(len(list(ctx.issues))):
        individual_error_dict = {
            "code": rule.code,
            "Table": str(list(ctx.issues)[i].table)[9:],
            "Columns": str(list(ctx.issues)[i].field),
            "ROW_ID": str(list(ctx.issues)[i].row),
        }
        individual_error_dict_df = pd.DataFrame([individual_error_dict])
        individual_error_df = pd.concat(
            [individual_error_df, individual_error_dict_df],
            ignore_index=True,
        )
    return error_dict, individual_error_df


def process_type1_issues(rule, ctx, individual_error_df):
    error_dict = {"code": rule.code, "number": len(list(ctx.issues)), "type": 1}
    issues = ctx.type1_issues
    row_df = issues.row_df
    print(f"{rule.code} {issues.table} {issues.columns}")
    print(pd.DataFrame({"columns": issues.columns, "Table": str(issues.table)[9:]}))
    print(issues.row_df)
    individual_error_dict_df = pd.DataFrame()
    # individual_error_dict_df["Table"] = str(issues.table)[9:]
    # individual_error_dict_df["Columns"] = issues.columns
    # individual_error_df = pd.concat(
    #     [individual_error_df, individual_error_dict_df],
    #     ignore_index=True,
    # )
    return error_dict


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

    df_issue_locs["tables_affected"] = issues.table

    df_issue_locs.reset_index(inplace=True)
    df_issue_locs.drop("index", axis=1, inplace=True)

    return df_issue_locs


class DataContainerWrapper:
    def __init__(self, value) -> None:
        self.value = value

    def __getitem__(self, name):
        return getattr(self.value, name.name)
