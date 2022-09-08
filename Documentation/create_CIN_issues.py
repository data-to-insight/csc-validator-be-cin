"""The code contained in this file automated the process of converting CIN rules 
from rows on an excel sheet to issues on this github repo."""

from github import Github

import pandas as pd

import os
import sys
import time

# generate a github token for yourself here https://github.com/settings/tokens
token = os.getenv("GITHUB_TOKEN", "write_your_token_here_keep_quotes")

# initialise the Github class
gh = Github(token)
# replace content of bracket with the path of the repo you want to push to.
repo = gh.get_repo("SocialFinanceDigitalLabs/CIN-validator")

# get rules file and confirm it's type.
rules_file = sys.argv[1]
while not rules_file.endswith("xlsx"):
    rules_file = input("Enter the name of the validation file, incl extension: ")
## Read validation rules into a dataframe.
df = pd.read_excel(rules_file, sheet_name=1)

"""
If rate limits are reached often and the operation frequently gets interrupted by Github, 
Continue from where you ended as shown. Start from the next index row (e.g row 92) and select all cols.
new_df = df.loc[92:, :].copy()
"""
# escape special characters so that they are not interpreted as Github markdown.
df["Validation check"] = (
    df["Validation check"].str.replace("<", "\<").str.replace(">", "\>")
)

# loop over dataframe
for index, row in df.iterrows():
    print(row["Sequence number"])
    # describe the properties of the github issue to be created at each row.
    repo.create_issue(
        # title should be of the form --> rule number: rule description
        title=f"Rule {row['Sequence number']} : {row['Message']}",
        # body is the text that includes the pseudocode of the rule
        body=row["Validation check"],
        # labels are any distinctive tags that might be useful during development.
        labels=[
            row["Error/ query"],
        ]
        # labels = [row['Error/ query'], row['Module']]
        # If label names frequently trigger validation errors, skip the labels part and create them using Github's UI
    )
    # wait between runs in order not to exceed Github's secondary rate limit.
    time.sleep(2)

## How to run this file.
"""
In your command line, do:
python create_CIN_issues.py rules_file_name_here
This is the rules file: https://assets.publishing.service.gov.uk/government/uploads/system/uploads/attachment_data/file/973883/Annex_A_Children_in_need_census_2021_to_2022_validation_rules_v1-2.xlsx
"""
