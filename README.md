# CIN-validator
The CIN validator is an open source, volunteer built tool that allows users to validate CIN census data year round via the command line, or using the browser based front end (URL HERE). It also provides a framework which other validation tools can easily be built on top of.

The functions are documented using sphinx format so that a docs website can be auto-generated if need be. Also, there is an increased use of python type-hints as a form of intrinsic documentation. This does not apply to test functions as they neither receive nor return data, in the strict sense.
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

### Update rule resources
- Run ` python get_uk_holidays.py` in the command line. This fetches the latest values of bank holidays into `cin_validator\england_holidates.py` (don't edit this file directly) for the rules that need them. Remember to convert \ to / if you are using a unix operating system.

### Update rules
- If any rules have been added or changed with respect to the previous year, create files for them in a rule folder named after the new validation year. For example, new or added rules for the 2023/24 validation year should be created in a folder named `cin2023_24`. Do not copy over rules that haven't changed.
- The __init__.py file contains the code that pulls in rules from the previous year and modifies them to meet the current year's specification. Copy across that init file whenever a folder for a new collection_year is created. Change the import to the name of the previous year's folder. 
- If the new specifications require that some rules are deleted, add their codes as strings to the `del_list` array in the current year's init file. Do not delete the rules manually. 
- Any new rules or modified rules should be added by creating a file for each rule and writing the modified code or new code. Even for small modifications, create a new file for the rule in the year where the modification was made instead of going backwards into the previous years and editing the original file.
- To run the modified set of rules from the command line interface, you can use the `-r` or `--ruleset` flag to specify the name of the rule folder that you wish to run. Otherwise, feel free to update the defaults of the commands so that they point to the new year's folder instead. For example, change `cin2022_23` to `cin2023_24`. 

## Make changes available to user
This part is a guide on how to update the frontend so that it reflects the changes that have been done in the backend.
- Delete the `dist` folder completely.
- Update the package version in the `pyproject.toml` file. (there is a section below to help you choose the new number.)
- run `poetry install` (installs project dependencies) and then `poetry shell` (ensures project runs in controlled environment) in the command line. You might have already done this when updating the rules.
- check that validation rules work as expected (pass) when you run `poetry python -m cin_validator test` in the command line.
- Then run `poetry build` in the command line. You will see that this creates a new `dist` folder.
- Push the pyproject.toml change to github and do a pull request. 
- When the version-number-change pull request is merged in, do a `release` by navigating to the release page from the right hand control bar on the repo homepage (click on `Releases` then `draft new release` in the top right hand corner)
- The release tag is created by including a `v` before the version number which you put in the `pyproject.toml` file. For example, if you filled in `version = "0.1.3"` then on Github, write `v0.1.3` as your release tag.
- Click on `generate release notes` at the top right of the main text box. Commit messages of all changes made since the last release will appear in the text box.
- Create a release title that starts with a ddmmyyy pattern to indicate the date of the release and then you can write a few words to describe the changes made since the last release.
- take the `.whl` file from the dist folder in this repo, go to the `public\bin\dist` location in the [frontend repo](https://github.com/data-to-insight/csc-validator-fe), delete the previous cin..`.whl` file in it and add this one.
- Search for the former wheel name on the frontend repo and update all locations where the wheel file name is referenced so that they now point to the new wheel file name with updated version number. [Here is an example](https://github.com/data-to-insight/csc-validator-fe/pull/177/files).
- Do a pull request to the frontend repo, containing your changes. 
- When the frontend pull request is merged in, the live tool will be updated automatically. You can confirm by checking that the version number in the footer of the web app has changed.
- You can watch the deployment process in the `Actions` tab on Github when your pull request is merged to the frontend.
- All done !

#### Notes about choosing version numbers
When changes are rules updates (add/delete/modify) or bug fixes, only the last part of the version number should be updated (e.g `v0.1.9` becomes `v0.1.10`). 
<br/>
The middle number is only updated when new features have been added (the changes enable user functionality that was not previously available).<br/>
Finally, the first part of the version number is changed when breaking changes are present (new version of tool is incompatible with previous version e.g when functions in the api, `rpc_main.py`, change.)
Read more about [semantic versioning here](https://semver.org/).