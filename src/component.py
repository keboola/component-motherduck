"""
Template Component main class.

"""

import logging
import os
from typing import Literal

import duckdb
from duckdb.duckdb import DuckDBPyConnection, DuckDBPyRelation
from keboola.component.base import ComponentBase, sync_action
from keboola.component.dao import (
    TableDefinition,
)
from keboola.component.exceptions import UserException
from keboola.component.sync_actions import SelectElement

from configuration import ColumnConfig, Configuration

DUCK_DB_DIR = os.path.join(os.environ.get("TMPDIR", "/tmp"), "duckdb")


class Component(ComponentBase):
    def __init__(self):
        super().__init__()
        self.params = Configuration(**self.configuration.parameters)
        self._connection = self.init_connection()

    def run(self):
        """
        Main execution code
        """

        in_table_definition = self.get_in_table()

        kbc_input_table_relation = self.create_temp_table(in_table_definition)

        if self.params.destination.incremental:
            self.create_db_table(
                database=self.params.database,
                db_schema=self.params.db_schema,
                table_name=self.params.destination.table,
                columns_config=self.params.destination.columns,
                mode="if_not_exists",
            )

            self.check_pks_consistency()

            if [col.destination_name for col in self.params.destination.columns if col.pk]:  # fmt: off
                # if primary key is defined, use UPSERT
                strategy = "INSERT OR REPLACE"
            else:
                strategy = "INSERT"

        else:
            self.create_db_table(
                database=self.params.database,
                db_schema=self.params.db_schema,
                table_name=self.params.destination.table,
                columns_config=self.params.destination.columns,
                mode="replace",
            )

            strategy = "INSERT"

        self._connection.execute(f"""
        {strategy} INTO {self.params.database}.{self.params.db_schema}.{self.params.destination.table}
        SELECT * FROM kbc_input_table_relation
        """)

        self._connection.close()

    def check_pks_consistency(self):
        """
        Check if the primary key columns defined in the configuration match the primary key columns in the destination table.
        """
        pk_selected = set(
            [col.destination_name for col in self.params.destination.columns if col.pk]
        )
        pk_md_result = self._connection.execute(
            f"""SELECT column_name
             FROM (SHOW {self.params.database}.{self.params.db_schema}.{self.params.destination.table})
             WHERE key IS NOT NULL"""
        ).fetchall()
        pk_md = set([val[0] for val in pk_md_result])
        if pk_selected != pk_md:
            raise UserException(
                f"Defined primary key columns do not match destination table."
                f"Defined: {pk_selected}, "
                f"Mother duck table columns: {pk_md}"
            )

    def create_db_table(
        self,
        database: str,
        db_schema: str,
        table_name: str,
        columns_config: list[ColumnConfig],
        mode: Literal["if_not_exists", "replace"],
    ) -> None:
        """
        Creates a db table based on column definitions.

        Args:
            database: The database name
            db_schema: The schema name
            table_name: The name of the table to create
            columns_config: List of ColumnConfig objects defining the columns
            mode: The mode for table creation, either "if_not_exists" or "replace"

        Returns:
            None
        """
        column_specs = []
        primary_key_columns = []

        for column in columns_config:
            column_definition = f"{column.destination_name} {column.dtype}"

            if not column.nullable:
                column_definition += " NOT NULL"

            if column.default_value is not None:
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
            query = f"CREATE OR REPLACE TABLE {database}.{db_schema}.{table_name} ( "
        elif mode == "if_not_exists":
            query = f"CREATE TABLE IF NOT EXISTS {database}.{db_schema}.{table_name} ( "
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

        self._connection.execute(query)

        return

    def get_in_table(self):
        in_tables = self.get_input_tables_definitions()
        if len(in_tables) != 1:
            raise UserException("Exactly one input table is expected.")
        return in_tables[0]

    def init_connection(self) -> DuckDBPyConnection:
        os.makedirs(DUCK_DB_DIR, exist_ok=True)
        config = dict(
            temp_directory=DUCK_DB_DIR,
            extension_directory=os.path.join(DUCK_DB_DIR, "extensions"),
            threads=self.params.threads,
            max_memory=f"{self.params.max_memory}MB",
            motherduck_token=self.params.token,
        )

        try:
            conn = duckdb.connect(database="md:", config=config)

        except Exception:
            raise UserException(
                f"Test connection failed, please check your configuration."
            )

        if not self.params.destination.preserve_insertion_order:
            conn.execute("SET preserve_insertion_order = false;").fetchall()

        return conn

    def create_temp_table(self, table_def: TableDefinition) -> DuckDBPyRelation:
        table = self._connection.read_csv(
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

    @sync_action("testConnection")
    def test_connection(self):
        pass  # just init connection

    @sync_action("list_databases")
    def list_databases(self):
        databases = self._connection.execute("SHOW ALL DATABASES").fetchall()
        return [SelectElement(d[0]) for d in databases]

    @sync_action("list_schemas")
    def list_schemas(self):
        schemas = self._connection.execute(f"""
        SELECT schema_name
        FROM information_schema.schemata
        WHERE catalog_name = '{self.params.database}'
        """).fetchall()

        return [SelectElement(s[0]) for s in schemas]

    @sync_action("list_tables")
    def list_tables(self):
        tables = self._connection.execute(f"""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_catalog = '{self.params.database}'
        AND table_schema = '{self.params.db_schema}'
        """).fetchall()

        return [SelectElement(t[0]) for t in tables]

    @sync_action("return_columns_data")
    def return_columns_data(self):
        if self.params.destination.columns:
            columns = self.params.destination.columns

        else:
            in_table = self.get_in_table()

            columns = []

            for name, definition in in_table.schema.items():
                columns.append(
                    ColumnConfig(
                        source_name=name,
                        destination_name=name,
                        dtype=definition.data_types.get("base").dtype,
                        pk=definition.primary_key or False,
                        nullable=definition.nullable,
                        default_value=definition.data_types.get("base").default,
                    ).model_dump()
                )

        return {
            "type": "data",
            "data": {"columns": columns},
        }


"""
        Main entrypoint
"""
if __name__ == "__main__":
    try:
        comp = Component()
        # this triggers the run method by default and is controlled by the configuration.action parameter
        comp.execute_action()
    except UserException as exc:
        logging.exception(exc)
        exit(1)
    except Exception as exc:
        logging.exception(exc)
        exit(2)
