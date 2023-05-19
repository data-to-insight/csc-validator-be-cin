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
`python -m cin_validator run-all <path to test data>`
-To run rules on the sample data and explore the output of the CLI:
`python -m cin_validator run-all path/to/your/cin/validator/CIN-validator/fake_data/fake_CIN_data.xml`
- To run rules on a file and select an instance of an error based on its ID:  
`python -m cin_validator run-all <path to test data> -e "<ERROR_ID as string>"`
- To convert a CIN XML file to it's respective CSV tables:  
`python -m cin_validator xmltocsv <path to test data>`

## Yearly tool updates
- Run `python get_uk_holidays.py` so that the latest values of bank holidays are drawn into `england_holidates.py` (don't edit this file directly) for the rules that need them.