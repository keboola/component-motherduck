import logging
import os
from typing import Literal

import duckdb
from duckdb.duckdb import DuckDBPyRelation
from keboola.component.dao import (
    TableDefinition,
)
from keboola.component.exceptions import UserException

DUCK_DB_DIR = os.path.join(os.environ.get("TMPDIR", "/tmp"), "duckdb")


class DuckConnection:
    def __init__(self, params):
        os.makedirs(DUCK_DB_DIR, exist_ok=True)
        self.params = params
        config = dict(
            temp_directory=DUCK_DB_DIR,
            extension_directory=os.path.join(DUCK_DB_DIR, "extensions"),
            threads=params.threads,
            max_memory=f"{params.max_memory}MB",
            motherduck_token=params.token,
        )

        try:
            self.connection = duckdb.connect(database="md:", config=config)

        except Exception:
            raise UserException(
                "Test connection failed, please check your configuration."
            )

    def create_db_table(
        self,
        mode: Literal["if_not_exists", "replace"],
    ) -> None:
        """
        Creates a db table based on column definitions.

        Args:
            mode: The mode for table creation, either "if_not_exists" or "replace"

        Returns:
            None
        """
        column_specs = []
        primary_key_columns = []

        for column in self.params.destination.columns:
            column_definition = f"{column.destination_name} {column.dtype}"

            if not column.nullable:
                column_definition += " NOT NULL"

            if column.default_value is not None and column.default_value != "":
                if column.dtype == "STRING":
                    # String values need quotes
                    column_definition += f" DEFAULT '{column.default_value}'"
                else:
                    # Numeric and boolean values don't need quotes
                    column_definition += f" DEFAULT {column.default_value}"

            column_specs.append(column_definition)

            # Keep track of primary key columns
            if column.pk:
                primary_key_columns.append(column.destination_name)

        if mode == "replace":
            query = (
                f"CREATE OR REPLACE TABLE "
                f"{self.params.db}.{self.params.db_schema}.{self.params.destination.table} ( "
            )
        elif mode == "if_not_exists":
            query = (
                f"CREATE TABLE IF NOT EXISTS "
                f"{self.params.db}.{self.params.db_schema}.{self.params.destination.table} ( "
            )
        else:
            raise UserException(
                f"Invalid mode: {mode}. Use 'if_not_exists' or 'replace'."
            )

        # Add all column definitions
        query += ", ".join(column_specs)

        # Add primary key constraint if any columns are marked as primary keys
        if primary_key_columns:
            query += f", PRIMARY KEY ({', '.join(primary_key_columns)})"

        # Finish the query
        query += ");"

        if self.params.debug:
            logging.debug(f"Executing query: {query}")

        self.connection.execute(query)

    def create_temp_table(self, table_def: TableDefinition) -> DuckDBPyRelation:
        table = self.connection.read_csv(
            path_or_buffer=table_def.full_path,
            delimiter=table_def.delimiter,
            quotechar=table_def.enclosure,
            header=table_def.has_header,
            names=list(table_def.schema),
            dtype={
                col.source_name: col.dtype for col in self.params.destination.columns
            },
        )
        return table
