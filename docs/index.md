---
layout: default
title: CIN validator docs
---



# The CIN validator tool
The repository for this tool can be found here: https://github.com/data-to-insight/CIN-validator

This open source tool allows users to validate CIN census data for the DfE return year round, rather than only during the return period as the is the case with the DfE tool. When run on data, the tool highlights parts of the data which violate the validation guidelines set out by the DfE for the CIN census return. The tool validates data according to the specification provided here (as of 2023) by the DfE: [CIN census validation guidelines](https://www.gov.uk/government/publications/children-in-need-census-2022-to-2023-specification#full-publication-update-history), but it needs to be updated yearly as return specification changes and relies on volunteers to do this. 

The tool is useable both in browser (URL TO BE DECIDED) and via the command line interface and validates data that is in the XML format that same data would be in for the DfE tool.

This tool was developed by a UK-wide group of analysts and local authority employees, in collaboration with Social Finance, a not for profit consultancy aiming to scale better solutions to complex social problems, and Data to Insight, a national project led by local authorities with support from the ADCS, DLUHC, DfE, and Ofsted to help local authorities make better use of data.

The continued existence and relevance of the tool relies on users, and those who are interested, contributing the to the maintenance and development of the tool. For instance, reporting bugs, fixing bugs, and updating the tool with new rules when the DfE releases new rules or changes current ones.

# Getting started 
The CIN validator tool can be used in two ways: via the command line interface, or using the browser based version of the tool, which is hosted here: PLACEHOLDER_FOR_URL

Using the browser based tool is about as simple dragging files in and clicking run, and is explained on the site. Using the CLI version takes a little setup, which is explained below. Both versions of the tool feature sample data so you can see how they work without using your own.

## Installing the command line tool

To use the CIN validator tool via the command line interface (CLI), you'll need a copy locally. You can use the sample data on codespaces if you have access, but you can't validate real data on codespaces as that would mean letting CIN data leave your local environment. To get a local copy, with a Git client installed or access to Git Bash, and in your IDE of choice, in the command line of your IDE run:
 `$ git clone https://github.com/data-to-insight/CIN-validator.git`
to clone a version of the tool locally, the same way you would clone any other repository. More on how to clone a repository can be found here: [cloning a GitHub repo](https://docs.github.com/en/repositories/creating-and-managing-repositories/cloning-a-repository). Alternatively, you can download a zip of the CIN validator tool from the front page of the repo and open that in your IDE. 

Once you have it open locally, use `pre-commit install` in the CLI to install the pre-commit hooks. These run some checks when you commit code via the CLI, for instance running Black to format the code.

Note: to run the code, you must use the CLI, you can't just run main.py like you would some scripts. Descriptions of how to do this are below.

## 'NO cin_validator module' - setting the PATH variable
When trying to run the code, you may sometimes get an error saying that module cin_validator cannot be found. If this is the case, you'll need to tell your IDE what directory to look in when running code, that is, the path to the folder on your computer where you have saved the file, or where you have cloned the file. Do this by entering into the terminal, replacing path/to/your/project with the path to your project: 

`export PYTHONPATH="${PYTHONPATH}:/path/to/your/project/"`

in my case, that is: 

`export PYTHONPATH="${PYTHONPATH}:/workspaces/CIN-validator/cin_validator"`

## Running the tool via the command line
You can't run the CIN validator by pressing the run button like you do with some Python scripts, you need to interact with it using the CLI. To do this, commands tend to look like:
`python -m cin_validator <command>`
 where `python` tells the command line you're using python, `-m cin_validator` says you want to select a module, and that module is the cin_validator, and where `<command>` is replaced with what you want to do. The current commands are as follows:

- To test that all the rules pass their Pytests and will validate data as expected:  
`python -m cin_validator test`
- To list all rules that are present:  
`python -m cin_validator list`
- To run rules on a file and generate a table of error locations:  
`python -m cin_validator run <path to test data>`
- To run rules on the sample data and explore the output of the CLI:
`python -m cin_validator run path/to/your/cin/validator/CIN-validator/fake_data/fake_CIN_data.xml`
- To run rules on a file and select an instance of an error based on its ID:  
`python -m cin_validator run <path to test data> -e "<ERROR_ID as string>"`
- To convert a CIN XML file to it's respective CSV tables:  
`python -m cin_validator xmltocsv <path to test data>`

## Data input structure
The tool currently takes in data as an XML as this is the format used to submit data to the DfE. A demonstration of how the data ingress works can be found here: [ingress demo](https://github.com/data-to-insight/CIN-validator/blob/main/cin_validator/demo_ingress_class.ipynb). This does require modules/tables and fields/columns to be labelled as they would to be for submission to the DfE, otherwise the tool may interpret your data wrongly or think you have fields/tables missing.

## Open source and other production environment use cases
The CIN validator tool backend lays out the framework for a tool which can validate most types of CS data, or other similar structures of data if key elements are tailored to that data (namely rules and ingress). As testament to this, we are in the process of moving a 903 return validation tool we offer over to the CIN validation framework for consistency and ease of maintenance. What this means is that it is entirely possible to utilise the framework provided here in another validation back-end setting, once the ingress, rules, and other minutiae are tailored to your validation needs.
 
# Joining the community and contributing to the codebase.
Anyone is welcome to contribute to the CIN validator tool, or use the code itself as it is open source. However, if you're interested in being involved, it may benefit you to join the community. Enquiries can be made at: datatoinsight.enquiries@gmail.com. The tools only exists because of the community that supports it. To keep functioning the tool needs: people to report bugs, people to fix bugs and maintain the code, people to code new rules, people to review submissions of code to the tool.

Data to Insight has also provided Python learning resources relevant to contributing to the tool here: https://www.datatoinsight.org/python.

## Reporting bugs
One of the most important things users can do is report bugs. The link for reporting bugs can be found here:  [report a bug](https://github.com/data-to-insight/CIN-validator/issues/new/choose). It asks users to explain the bug, what issues it causes, and how to reproduce it. It is then the job of volunteers to fix the bug, and others to review the big-fixing code and get it merged to the production code. Also, when reporting bugs in the CIN validation tool we recommend contacting us: [email us](datatoinsight.enquiries@gmail.com) so we can set up a virtual meeting to discuss, work through, and fix those bugs.

## Contributing code - Rule types
If you want to update, big-fix, or add rules, you'll need to get familiar with the different types of validation rules. We have made templates for types of validation rules that check similar things to ensure that they return the right thing in the right way to report errors. There are a number of different rule types in the CIN validator. These rule types exist for a number of reasons: making it easier to provide templates to code new, similar rules, and because different rule types require different bits of information in order to link failing cells to the original data to indicate errors to users. The preferred method of writing a validation rule is to copy/paste the code of a rule of the same type that is already in the code, and then alter the code to fit the rule you're adding. That will involve changing the definitions of the variables which are assigned tables and columns relevant to your rule (under the import statements), updating the `@rule_definition` decorator, updating the `validate` function in your rule file, and filling out the `test_validate` function to appropriately test your validate function.

the rule splits are as follows:
- Type 0 rules validate data in a single column.
- Type 1 rules validate data within one table, but across multiple columns.
- Type 2 rules validate data across multiple tables, requiring merging those tables.
- Type 3 rules validate data within a group, for instance a CINplan group, this requires, at least, merging a table on itself.
- LA level rules validate data across the entire data set for an LA. 

 There are in depth guides for beginners on how to code rules of each type:
- [Type 0](https://www.datatoinsight.org/cinrules)
- [Type 1](https://www.datatoinsight.org/type-1-cin-rules)
- [Type 2](https://www.datatoinsight.org/type-2-cin-rules)
- [Type 3](https://www.datatoinsight.org/type-3-cin-rules)


When contributing rules, it can be useful to look at similar rules to the one you are coding then contributing to the CIN validator in order to see how you might code a particular validation rule. A list of all rules and the kind of validation they do can be found here: [rule patterns](https://github.com/data-to-insight/CIN-validator/blob/main/Documentation/RULE_PATTERNS.MD). Using this can provide a framework for new rules.

There are also demonstrations of some of the types of validation used for rule coding. These are available as Jupyter notebooks here: [rule type demos](https://github.com/data-to-insight/CIN-validator/tree/main/Documentation/rule_logic_demos).

If you alter a rule or add one, it's important to follow the naming convention used in the rules directory of the CIN validator so that it's picked up and run: rule_xxx.py where xxx is replaced with the rule code. It is also important that you write an appropriate test for your rule, using the template provided by other rules. To do this, you'll need to use the template found in any rule of the same type, and fill it out to suit your rule. That means making a DataFrame (or set of data-frames) that should pass and fail in known rows to make sure that your validate function allows data to pass which should, and fails data that should fail. You'll then need to fill out the assert statements and expected DataFrame to match this. To check your validation code works as intended, try and account for as many possible cases as you can in your test DataFrames. Also, don't change your assert statements just so they pass! Make sure you know you're returning what you expect and why. 

## Debugging multiple failing tests.
Sometimes, due to a change in the behaviour of a dependency or a change done in the core functions of the tool, multiple validation rules might fail for a variety of reasons. 
It is wise to separate out the files of the rules which fail so that you can debug progressively.

In the `__main__.py` file, which controls the command line functionality, find the code for the `test` command and replace the `pytest.main(test_files)` line with this instead
```
failed_files = []
for file_path in test_files:
    result = pytest.main([file_path])
    if result != pytest.ExitCode.OK:
        failed_files.append(file_path)

with open("files_failed.json", "w") as f:
    json.dump(failed_files, f)
```
Next, run `python -m cin_validator test`. This creates a json file containing the absolute filepaths of the rules whose tests failed. You can now run only the rules which failed by creating a new command in main.py as such:

```
@cli.command(name="retest")
def retest():
    with open("files_failed.json", "r") as f:
        filepaths = json.load(f)
    pytest.main(filepaths)
```

Now, running `python -m cin_validator retest` only runs the rules which failed and as you debug, it is easier to track whether or not your changes have worked since it is a shorter list of rules to look through.

When a number of rules pass on the retest command, you might want to filter it out again so that you only have the rules that still fail. To do that, run `python -m cin_validator test` again so that the `files_failed.json` file is updated.
The files_failed.json is a temporary file for debugging purposes only and can easily be regenerated so do not commit it when you push your changes to the Github repo. 

## Pushing changes to the live version
Refer to the README file on the landing page of the repo for the up-to-date process.

# Road-map
Community maintenance: The CIN validation tool relies on the community of analysts who use it to be an effective tool. Currently this community is relied on for bug reports, bug fixing, updating the tool to write new rules, and maintaining the current ones. This is done in collaboration with one paid member of staff at Data to Insight and one at Social Finance who have to goal of facilitating and aiding analysts in being able to work on the tool. As part of this, the road-map for the CIN validator also includes building the community of analysts who work on and use the tool, building up the Python skills of interested analysts, and encouraging interested analysts to join other Python related projects with Data to Insight, who are currently working on an online data analysis and visualisation pipeline, allowing analysts without access to Python in their LA to code Python and then access the output sin their browser.

CIN validator extensions: We are currently in the process of organising how to port the 903 validation tool over to the same framework as the CIN validation tool. Doing so will improve our insight into how we can build other validation tools on top of the CIN/903 validator more easily.

CSV functionality: the tool should eventually be able to take in CSVs as well as XMLs, it should also be able to output XMLs as the current method of conversion offered by the DfE is very slow and clunky.
