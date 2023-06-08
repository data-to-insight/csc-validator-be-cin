# CIN-validator
The CIN validator is an open source, volunteer built tool that allows users to validate CIN census data year round via the command line, or using the browser based front end (URL HERE). It also provides a framework which other validation tools can easily be built on top of.

More extensive documentation can be found here: https://data-to-insight.github.io/CIN-validator/

## Setup
This repo can be opened and run in a codespaces instance or cloned locally using `git clone https://github.com/data-to-insight/CIN-validator.git`
If it is cloned locally, use `pre-commit install` to install the pre-commit hooks.

## Run
- To test that all the rules pass their Pytests and will validate data as expected:  
`python -m cin_validator test`
- To list all rules that are present:  
`python -m cin_validator list`
- To run rules on a file and generate a table of error locations:  
`python -m cin_validator run <path to test data>`
-To run rules on the sample data and explore the output of the CLI:
`python -m cin_validator run path/to/your/cin/validator/CIN-validator/fake_data/fake_CIN_data.xml`
- To run rules on a file and select an instance of an error based on its ID:  
`python -m cin_validator run <path to test data> -e "<ERROR_ID as string>"`
- To convert a CIN XML file to it's respective CSV tables:  
`python -m cin_validator xmltocsv <path to test data>`

## Yearly tool updates

### update rule resources
- Run ` python get_uk_holidays.py` in the command line. This fetches the latest values of bank holidays into `cin_validator\england_holidates.py` (don't edit this file directly) for the rules that need them. Remember to convert \ to / if you are using a unix operating system.

### update rules
- If any rules have been added or changed with respect to the previous year, create files for them in a rule folder named after the new validation year. For example, new or added rules for the 2023/24 validation year should be created in a folder named `cin2023_24`. Do not copy over rules that haven't changed.
- Follow the instructions in `cin_validator\ruleset.py`. This step ensures that your folder (containing the recent rule updates) is linked to the previous years and can inherit those rules which haven't changed. It will automatically sync even if more rules are added to the folder later.
- If any rules have been deleted, do not delete the rule files from the previous year. Instead, add the rule codes *as strings* to the corresponding list in `cin_validator\ruleset.py`. For example, if the specification of 2023/24 states that few rules which were present in the previous year should no longer be considered - edits should be done on this line `ruleset_updates["cin2023_24"] = {"deleted": [], "ruleset": cin23_24}`. 
- Update ruleset defaults in `cin_validator\__main__.py` by updating `default="cin2022_23"` to that of the relevant year. For example, for the year 2023/24 use `default="cin2023_24"`

## make changes available to user
- delete the `dist` folder completely.
- run `poetry install` (installs project dependencies) and then `poetry shell` (ensures project runs in controlled environment) in the command line. You might have already done this when updating the rules.
- check that validation rules work as expected (pass) when you run `poetry python -m cin_validator test` in the command line.
- Then run `poetry build` in the command line. You will see that this creates a new `dist` folder.
- take the `.whl` file from the dist folder in this repo, go to the `public\bin\dist` location in the frontend repo, delete the cin...whl file in it and add this one.
- Do a pull request to the frontend repo, containing your changes. When that is merged in, the tool will be updated automatically so that your rules updates from the backend (which were zipped up in the wheel file), are now publicly available.
- You can watch the deployment process in the `Actions` tab on Github when your pull request is merged to the frontend.
- All done !