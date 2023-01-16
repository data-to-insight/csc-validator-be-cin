---
layout: default
title: CIN validator docs
---

# The CIN validator tool
This open source tool allows users to validate CIN census data for the DfE return year round, rather than only during the return period as the is the case with the DfE tool. The tool validates data according to the specification provided here (as of 2023) by the DfE: https://www.gov.uk/government/publications/children-in-need-census-2022-to-2023-specification#full-publication-update-history, but it needs to be updated yearly as return specification changes and relies on volunteers to do this. The tools is useable both in browser (URL TO BE DECIDED) and via the command line interface.

This tool was developed by a UK wide group of analysts and local authority employees, in collaboration with Social Finance, a not for profit consultancy aiming to tackle social problems in the UK and Globally, and Data to Insight, a national project led by local authorities with support from the ADCS, DLUHC, DfE and Ofsted to help local authorities make better use of data.

The continued existence and relevance of the tool relies on users, and those who are interested, contributing the to the maintenance and development of the tool. For instance, reporting bugs, fixing bugs, and updating the tool with new rules when the DfE releases new rules or changes current ones.

# Installing the command line tool

To use the CIN validator tool via the command line interface (CLI), you'll need a copy locally. You can't use a version on codespaces as that would mean letting CIN data leave your local environment. To do this, with a Git client installed, and in your IDE of choice, in the command line of your IDE run:
 ` $ git clone https://github.com/data-to-insight/CIN-validator.git`
to clone a version of the tool locally. Alternatively, you can download a zip of the CIN validator tool from the front page of the repo and open that in your IDE. 

If it is cloned locally, use `pre-commit install` in the CLI to install the precommit hooks.

## 'NO cin_validator module - setting the PATH variable
You may sometimes get an error saying that module cin_validator cannot be found. If this is the case, you'll need to tell Codespaces what directory to look in when running code. Do this by entering into the terminal, replacing path/to/your/project with the path to your project: 

`export PYTHONPATH="${PYTHONPATH}:/path/to/your/project/"`

in my case, that is: 

`export PYTHONPATH="${PYTHONPATH}:/workspaces/CIN-validator/cin_validator"`

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

## Data input structure
The tool currently takes in data as an XML as this is the format used to submit data to the DfE. A demonstration of how the data ingress works can be found here: https://github.com/data-to-insight/CIN-validator/blob/main/cin_validator/demo_ingress_class.ipynb. This does reqauire modules/tables and fields/columns to be labelled as they need to be for submission to the DfE.
 
# Joining the community and contributing to the codebase.
Anyone is welcome to contribute to the CIN validator tool, or use the code itself as it is open source. However, if you're interetsed in being involved, it may benefit you to join the community. Enquiries can be made at: datatoinsight.enquiries@gmail.com. The tools only exists because of the community that supports it. To keep functioning the tool needs: people to report bugs, people to fix bugs and maintian the code, people to code new rules, people to review submissions of code to the tool.

Data to Insight has also provided Python learning resources relevant to contributing to the tool here: https://www.datatoinsight.org/python. This page also includes in depth guides for beginners on how to code rules of each type
- https://www.datatoinsight.org/cinrules
- https://www.datatoinsight.org/type-1-cin-rules
- https://www.datatoinsight.org/type-2-cin-rules
- https://www.datatoinsight.org/type-3-cin-rules


## Reporting bugs
One of the most important things users can do is report bugs. The link for reporting bugs can be found here:  https://github.com/data-to-insight/CIN-validator/issues/new/choose. It asks users to explain the bug, what issues it causes, and how to reproduce it. It is then the job of volunteers to fix the bug, and others to review the big-fixing code and get it merged to the production code.

## Rule types
There are a number of different rule types in the CIN validator. These rule types exist for a number of reasons: making it easier to provide templates to code new, similar rules, and because different rule types require different bits of information in order to link failiong cells to the original data to indicate errors to users. 

the rule splits are as follows:
- Type 0 rules validate data in a single column.
- Type 1 rules validate data within one table, but across multiple columns.
- Type 2 rules validate data across multiple tables, requiring merging those tables.
- Type 3 rules validate data within a group, for instance a CINplan group, this requires, at least, merging a table on itself.
- LA level rules validate data across the entire data set for an LA. 

When contributing rules, it can be useful to look at similar rules to the one you are coding then contributing to the CIN validator in order to see how you might code a particular validation rule. A list of all rules and the kind of validation they do can be found here: https://github.com/data-to-insight/CIN-validator/blob/main/Documentation/RULE_PATTERNS.MD. Using this can provide a framework for new rules.

There are also demonstrations of some of the types of validation used for rule coding. These are avaliable as Jupyter notebooks here: https://github.com/data-to-insight/CIN-validator/tree/main/Documentation/rule_logic_demos

