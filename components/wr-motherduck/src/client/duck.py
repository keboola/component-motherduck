import logging
import os

import duckdb
from duckdb.duckdb import ConstraintException, DuckDBPyRelation
from keboola.component.dao import (
    TableDefinition,
)
from keboola.component.exceptions import UserException

DUCK_DB_DIR = os.path.join(os.environ.get("TMPDIR", "/tmp"), "duckdb")


class DuckConnection:
    def __init__(self, params):
        os.makedirs(DUCK_DB_DIR, exist_ok=True)
        self.params = params
        self.destination = None

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
            raise UserException("Test connection failed, please check your configuration.")

    def upload_table(self, in_table_definition, destination: str):
        self.destination = destination

        # table name is referenced in the query
        kbc_input_table_relation = self.create_temp_table(in_table_definition)  # noqa: F841

        try:
            strategy = "INSERT"
            if self.params.destination.incremental:
                self.create_db_table()
                self._check_pks_consistency()

                if [col.destination_name for col in self.params.destination.columns if col.pk]:
                    # if primary key is defined, use UPSERT
                    strategy = "INSERT OR REPLACE"
            else:
                self.create_db_table(replace_existing=True)

            columns = ", ".join([f"{col.source_name}" for col in self.params.destination.columns])

            self.connection.execute(f"""
            {strategy} INTO {self.destination}
            SELECT {columns} FROM kbc_input_table_relation
            """)
        except ConstraintException as e:
            raise UserException(f"Error during data load: {e}") from e
        finally:
            self.connection.close()

    def _check_pks_consistency(self):
        """
        Check if the primary key columns defined in the configuration
        match the primary key columns in the destination table.
        """
        pk_selected = set([col.destination_name for col in self.params.destination.columns if col.pk])
        pk_md_result = self.connection.execute(
            f"""SELECT column_name
             FROM (SHOW {self.destination})
             WHERE key IS NOT NULL"""
        ).fetchall()
        pk_md = set([val[0] for val in pk_md_result])
        if pk_selected != pk_md:
            raise UserException(
                f"Defined primary key columns do not match destination table."
                f"Defined: {pk_selected}, "
                f"Mother duck table columns: {pk_md}"
            )

    def create_db_table(self, replace_existing: bool = False) -> None:
        """
        Creates a db table based on column definitions.

        Args:
            replace_existing: If True, replace the existing table.

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

        if replace_existing:
            query = f"CREATE OR REPLACE TABLE {self.destination} ( "
        else:
            query = f"CREATE TABLE IF NOT EXISTS {self.destination} ( "

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
            dtype={col.source_name: col.dtype for col in self.params.destination.columns},
        )
        return table
