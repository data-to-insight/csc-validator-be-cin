# CIN-validator

## Setup
This repo can be opened and run in a codespaces instance or cloned locally using `git clone https://github.com/data-to-insight/CIN-validator.git`
If it is cloned locally, do `pre-commit install` to install the precommit hooks.

## Run
- To test all rules:  
`python -m cin_validator test`
- To list all rules that are present:  
`python -m cin_validator test`
- To run rules on a file and generate a table of error locations:  
`python -m cin_validator run-all <path to test data>`
- To run rules on a file and select an instance of an error based on its ID:  
`python -m cin_validator run-all <path to test data> -e "<ERROR_ID as string>"`
- To convert a CIN XML file to it's respective CSV tables:  
`python -m cin_validator xmltocsv <path to test data>`
