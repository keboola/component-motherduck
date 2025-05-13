MotherDuck Writer
=============

A Keboola component that writes data from KBC to MotherDuck.

**Table of Contents:**

[TOC]

Functionality Notes
===================

This component writes data from KBC to MotherDuck. It allows you to specify which columns to write, their data types, nullability, and default values. The component supports full load, append, and upsert modes.

Prerequisites
=============

You must have a MotherDuck account and obtain an access token for authentication. More information on how to get the token can be found in the [MotherDuck documentation](https://motherduck.com/docs/key-tasks/authenticating-and-connecting-to-motherduck/authenticating-to-motherduck/#authentication-using-an-access-token).

Features
========

| **Feature**             | **Description**                                                   |
|-------------------------|-------------------------------------------------------------------|
| Authentication          | Connect using MotherDuck access tokens                            |
| Database Selection      | Select specific database and schema for operations                |
| Load Types              | Support for both incremental and full load operations             |
| Column Configuration    | Detailed column mapping with type control                         |
| Primary Key Support     | Define primary keys for data integrity                            |
| Data Type Management    | Support for various DuckDB data types                             |
| Nullable Columns        | Configure whether columns can contain NULL values                 |
| Default Values          | Set default values for columns                                    |
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
| database        | Target database in MotherDuck                 | Yes          |
| db_schema       | Target schema within the database             | Yes          |
| debug           | Enable detailed logging (default: false)      | No           |
| threads         | Number of threads to use (default: 1)         | No           |
| max_memory      | Maximum memory usage in MB (default: 256)     | No           |

Table Configuration
------------------

For each table you want to configure, you need to specify:

| **Parameter**   | **Description**                               | **Required** |
|-----------------|-----------------------------------------------|--------------|
| destination.table | Target table name                          | Yes          |
| destination.load_type | Load type: "incremental_load" or "full_load" (default: "incremental_load") | No |
| destination.columns | Column configurations (see below)        | Yes          |

Column Configuration
-------------------

For each column in a table, you can specify:

| **Parameter**     | **Description**                             | **Required** |
|-------------------|---------------------------------------------|--------------|
| source_name       | Name of the source column                   | Yes          |
| destination_name  | Name of the column in the destination table | Yes          |
| dtype             | Data type (VARCHAR, INTEGER, etc.)          | No           |
| pk                | Whether column is a primary key             | No           |
| nullable          | Whether column can contain NULL values      | No           |
| default_value     | Default value for the column                | No           |

Supported Data Types
-------------------

The component supports various DuckDB data types including:
- BIGINT, INTEGER, SMALLINT, TINYINT (and unsigned variants)
- BOOLEAN, BIT
- VARCHAR, JSON, UUID
- DECIMAL, DOUBLE, FLOAT
- DATE, TIME, TIMESTAMP
- And more

Output
======

The component outputs data to the specified MotherDuck database and schema, according to the configured tables and columns.

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
