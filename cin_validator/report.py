import pandas as pd
from .cin_validator_class import CINvalidationSession, include_issue_child

def include_issue_value(issue_df, cin_data):
    include_issue_child(issue_df, cin_data)
    
    """
    :param DataFrame issue_df: complete data about all issue locations.
    :param dict cin_data: dictionary of dataframes generated when cin xml is converted to tabular format.
    """

    la_level_issues = issue_df[issue_df["tables_affected"].isna()]
    header_issues = issue_df[issue_df["tables_affected"] == "Header"]
    tables_with_childid = [la_level_issues, header_issues]
    for table in issue_df["tables_affected"].dropna().unique():
        if table == "Header":
            # the header table doesn't contain child id. It is like metadata
            continue
        table_df = issue_df[issue_df["tables_affected"] == table]

        # get index values of the rows that fail.
        # some ROW_ID values exist as ints and others as strs. Unify so that .unique() doesn't contain doubles.
        table_rows = table_df["ROW_ID"].astype("int").unique()

        # naming the index of the data allows it to be mapped back to the issue_df
        table_data = cin_data[table]
        table_data.index.name = "ROW_ID"
        table_data.reset_index(inplace=True)
        # select the data for the rows with appear in issue_df and get the child ids
        linker_df = table_data.iloc[table_rows][["LAchildID", "ROW_ID"]]

        # work around: ensure that columns from both sources have the same type to prevent merge error
        table_df["ROW_ID"] = table_df["ROW_ID"].astype("int64")
        linker_df["ROW_ID"] = linker_df["ROW_ID"].astype("int64")
        # map the child ids back to issue_df
        table_df = table_df.merge(linker_df, on=["ROW_ID"], how="left")

        # save the result
        tables_with_childid.append(table_df)

    # regenerate issue_df from its updated constituent tables
    issue_df = pd.concat(tables_with_childid)

    return issue_df