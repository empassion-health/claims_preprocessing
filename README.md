[![Apache License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0) ![dbt logo and version](https://img.shields.io/static/v1?logo=dbt&label=dbt-version&message=1.x&color=orange)

# Claims Preprocessing

This is the Tuva Project's Claims Preprocessing Engine, which is a dbt project to profile and transform raw claims data so that it's ready for analytics. It's based on a methodology we developed at Tuva based (1) our experience working with healthcare claims data and (2) published papers on the topic.  From a high-level it does the following things:
- Assigns encounter types to individual claim lines using logic based on bill type code, revenue code, and place of service code
- Groups individual claims into a single encounter by merging claims with overlapping or adjacent dates, for the same patient, provider, condition, etc.
- Crosswalks professional claims to institutional encounters

Check out the [DAG](https://tuva-health.github.io/chronic_conditions/#!/overview?g_v=1) for this data mart

Knowledge Base:
- We'll post the methodology for this preprocessing engine soon!
- Check out the [data model](https://thetuvaproject.com/docs/data-models/claims-input-layer) used for this preprocessing engine


## Pre-requisites
1. You have claims data (e.g. medicare, medicaid, or commercial) in a data warehouse
2. You have mapped your claims data to the [claim input layer](https://docs.google.com/spreadsheets/d/1NuMEhcx6D6MSyZEQ6yk0LWU0HLvaeVma8S-5zhOnbcE/edit?usp=sharing)
    - The claim input layer is at a claim line level and each claim id and claim line number is unique
    - The eligibility input layer is unique at the month/year grain per patient and payer
    - Revenue code is 4 digits in length
2. You have [dbt](https://www.getdbt.com/) installed and configured (i.e. connected to your data warehouse)

[Here](https://docs.getdbt.com/dbt-cli/installation) are instructions for installing dbt.

## Getting Started
Complete the following steps to configure the package to run in your environment.

1. [Clone](https://docs.github.com/en/repositories/creating-and-managing-repositories/cloning-a-repository) this repo to your local machine or environment
2. Create a database called 'tuva' in your data warehouse
    - Note: this is optional, see step 4 for further detail
3. Configure [dbt_project.yml](/dbt_project.yml)
    - Fill in vars (variables):
        - source_name - description of the dataset feeding this project
        - input_database - database where sources feeding this project are stored
        - input_schema - schema where sources feeding this project is stored
        - output_database - database where output of this project should be written.  
        We suggest using the Tuva database but any database will work.
        - output_schema - name of the schema where output of this project should be written
4. Review [sources.yml](models/sources.yml).  The table names listed are the same as in the Tuva data model (linked above).  If you decided to rename these tables:
    - Update table names in sources.yml
    - Update table name in medical_claim and eligibility jinja function
5. Execute `dbt build` to load seed files, run models, and perform tests.

## Usage Example
Sample dbt command specifying new variable names dynamically:

```
dbt build --vars '{input_database: extract_from_database, input_schema: extract_from_schema, output_database: insert_into_database, output_schema: insert_into_schema}'
```


## Contributions
Have an opinion on the mappings? Notice any bugs when installing and running the package? 
If so, we highly encourage and welcome contributions! 

Join the conversation on [Slack](https://tuvahealth.slack.com/ssb/redirect#/shared-invite/email)!  We'd love to hear from you on the #claims-preprocessing channel.

## Database Support
This package has been written for Snowflake.  Redshift is available [here](https://github.com/tuva-health/claims_preprocessing_redshift)
