MotherDuck Extractor
=================

A Keboola component that extracts data from MotherDuck to KBC.

**Table of Contents:**

[TOC]

Functionality Notes
===================

This component extracts data from MotherDuck to KBC. It allows you to select specific tables, columns, or use custom queries to fetch data from your MotherDuck database.

Prerequisites
=============

You must have a MotherDuck account and obtain an access token for authentication. More information on how to get the token can be found in the [MotherDuck documentation](https://motherduck.com/docs/key-tasks/authenticating-and-connecting-to-motherduck/authenticating-to-motherduck/#authentication-using-an-access-token).

Features
========

| **Feature**             | **Description**                                                   |
|-------------------------|-------------------------------------------------------------------|
| Authentication          | Connect using MotherDuck access tokens                            |
| Database Selection      | Select specific database and schema for operations                |
| Data Selection          | Multiple modes: All data, select columns, or custom queries       |
| Load Types              | Support for both incremental and full load operations             |
| Primary Key Support     | Define primary keys for data integrity                            |
| Preview Capabilities    | Preview table data and query results                              |
| Debug Mode              | Enable detailed logging for troubleshooting                       |
| Multi-threading         | Configurable number of threads for performance optimization       |
| Memory Management       | Control maximum memory usage during operations                    |

Configuration
=============

Connection Parameters
--------------------

| **Parameter**   | **Description**                               | **Required** |
|-----------------|-----------------------------------------------|--------------|
| #token          | MotherDuck access token for authentication    | Yes          |
| db              | Target database in MotherDuck                 | Yes          |
| db_schema       | Target schema within the database             | Yes          |
| debug           | Enable detailed logging (default: false)      | No           |
| threads         | Number of threads to use (default: 1)         | No           |
| max_memory      | Maximum memory usage in MB (default: 256)     | No           |

Data Selection Configuration
---------------------------

The component supports three modes for selecting data:

| **Mode**        | **Description**                                           |
|-----------------|-----------------------------------------------------------|
| all_data        | Extract all data from the selected table                  |
| select_columns  | Extract only specified columns from the selected table    |
| custom_query    | Use a custom SQL query to extract data                    |

For the "select_columns" mode, you need to specify which columns to extract. For the "custom_query" mode, you provide a SQL query where "in_table" will be replaced with the actual table path.

Destination Configuration
------------------------

| **Parameter**            | **Description**                                           | **Required** |
|--------------------------|-----------------------------------------------------------|--------------|
| load_type                | "incremental_load" or "full_load" (default: "incremental_load") | No |
| primary_key              | Columns to use as primary keys for incremental loading    | No          |
| table_name               | Custom name for the output table                          | No          |
| preserve_insertion_order | Whether to preserve the order of rows (default: true)     | No          |

Output
======

The component extracts data from MotherDuck and loads it into KBC tables according to the configured settings.

Development
-----------

To customize the local data folder path, replace the `CUSTOM_FOLDER` placeholder with your desired path in the `docker-compose.yml` file:

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    volumes:
      - ./:/code
      - ./CUSTOM_FOLDER:/data
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Clone this repository, initialize the workspace, and run the component using the following
commands:

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
git clone https://github.com/keboola/component-motherduck component-motherduck
cd component-motherduck
docker-compose build
docker-compose run --rm dev
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Run the test suite and perform lint checks using this command:

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
docker-compose run --rm test
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Integration
===========

For details about deployment and integration with Keboola, refer to the
[deployment section of the developer
documentation](https://developers.keboola.com/extend/component/deployment/).
