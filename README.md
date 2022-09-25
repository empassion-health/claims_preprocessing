[![Apache License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0) ![dbt logo and version](https://img.shields.io/static/v1?logo=dbt&label=dbt-version&message=1.x&color=orange)

# Claims Preprocessing

This is the Tuva Project's Claims Preprocessing data mart, which is a dbt project to test and transform raw claims data so that it's ready for analytics. It's based on a methodology we developed at Tuva using (1) our experience working with healthcare claims data and (2) published papers on the topic.  

From a high-level claims preprocessing groups claims into encounters (i.e. visits) by:
- Assigning encounter types to individual claim lines using logic based on bill type code, revenue code, and place of service code
- Grouping individual claims into a single encounter by merging claims with overlapping or adjacent dates, for the same patient, provider, condition, etc.
- Crosswalking professional claims to institutional encounters

We'll post the DAG for this preprocessing engine soon!

Knowledge Base:
- We'll post the full methodology for claims preprocessing soon!
- Check out the output [data model](https://thetuvaproject.com/docs/category/core) for this preprocessing engine

## Pre-requisites
1. You have healthcare data (e.g. claims data) in a data warehouse (e.g. Snowflake)
2. You have mapped your claims data to [Claims Input Layer](https://thetuvaproject.com/docs/data-models/claims-input-layer)
3. You have [dbt](https://www.getdbt.com/) installed and configured (i.e. connected to your data warehouse)

[Here](https://docs.getdbt.com/dbt-cli/installation) are instructions for installing dbt.

## Getting Started
Complete the following steps to configure the data mart to run in your environment.

1. [Clone](https://docs.github.com/en/repositories/creating-and-managing-repositories/cloning-a-repository) this repo to your local machine or environment
2. Configure [dbt_project.yml](/dbt_project.yml)
    - Profile: set to 'default' by default - change this to an active profile in the profile.yml file that connects to your data warehouse 
    - Fill in the following vars (variables):
      - source_name - description of the dataset feeding this project 
      - input_database - database where sources feeding this project are stored 
      - input_schema - schema where sources feeding this project is stored 
      - output_database - database where output of this project should be written. We suggest using the Tuva database but any database will work. 
      - output_schema - name of the schema where output of this project should be written
3. Execute `dbt build` to load seed files, run models, and perform tests.

Alternatively you can execute the following code and skip step 2b and step 3.
```
dbt build --vars '{input_database: my_database, input_schema: my_input, output_database: my_other_database, output_schema: i_love_data}'
```


## Contributions
Have an opinion on the mappings? Notice any bugs when installing and running the package? 
If so, we highly encourage and welcome contributions!

## Community
Join our growing community of healthcare data practitioners on [Slack](https://join.slack.com/t/thetuvaproject/shared_invite/zt-16iz61187-G522Mc2WGA2mHF57e0il0Q)!

## Database Support
This package has been tested on Snowflake and Redshift.
