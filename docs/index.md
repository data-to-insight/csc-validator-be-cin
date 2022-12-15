---
layout: default
title: CIN validator docs
---

#Installing the command line tool

To use the CIN validator tool via the command line interface (CLI), you'll need a copy locally. You can't use a version on codespaces as that would mean letting CIN data leave your local environment. To do this, with a Git client installed, and in your IDE of choice, in the command line of your IDE run:
 ` $ git clone https://github.com/data-to-insight/CIN-validator.git`
to clone a version of the tool locally. Alternatively, you can download a zip of the CIN validator tool from the front page of the repo and open that in your IDE. 

If it is cloned locally, use `pre-commit install` in the CLI to install the precommit hooks.

## Running the tool via the command line
You can't run the CIN validator by pressing the run button like you do with some Python scripts, you need to interact with it using the CLI. To do this, commands tend to look like:
  `python -m cin_validator <command>`
 where `python` tells the command line you're using python, `-m cin_validator` says you want to select a module, and that module is the cin_validator and where `<command>` is replaced with what you want to do. The current commands are as follows:
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
 
 
