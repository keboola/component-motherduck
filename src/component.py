"""
Template Component main class.

"""

import logging
import os
from typing import Literal

import duckdb
from duckdb.duckdb import DuckDBPyConnection, DuckDBPyRelation
from kbcstorage.client import Client as StorageClient
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

        # table name is referenced in the query
        kbc_input_table_relation = self.create_temp_table(in_table_definition)  # noqa: F841

        try:
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

            columns = ", ".join(
                [f"{col.source_name}" for col in self.params.destination.columns]
            )

            self._connection.execute(f"""
            {strategy} INTO {self.params.database}.{self.params.db_schema}.{self.params.destination.table}
            SELECT {columns} FROM kbc_input_table_relation
            """)
        except duckdb.duckdb.ConstraintException as e:
            raise UserException(f"Error during data load: {e}") from e
        finally:
            self._connection.close()

    def check_pks_consistency(self):
        """
        Check if the primary key columns defined in the configuration
        match the primary key columns in the destination table.
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

        if self.params.debug:
            logging.debug(f"Executing query: {query}")

        self._connection.execute(query)

        return

    def get_in_table(self):
        in_tables = self.get_input_tables_definitions()
        if len(in_tables) != 1:
            raise UserException(
                f"Exactly one input table is expected. Found: {[t.destination for t in in_tables]}"
            )
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
                "Test connection failed, please check your configuration."
            )

        if (
            self.params.destination
            and not self.params.destination.preserve_insertion_order
        ):
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

    def _init_storage_client(self) -> StorageClient:
        storage_token = self.environment_variables.token
        storage_client = StorageClient(self.environment_variables.url, storage_token)
        return storage_client

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
            columns = [col.model_dump() for col in self.params.destination.columns]

        else:
            if len(self.configuration.tables_input_mapping) != 1:
                raise UserException(
                    "Exactly one input table is expected. Found: "
                    f"{[t.destination for t in self.configuration.tables_input_mapping]}"
                )

            table_id = self.configuration.tables_input_mapping[0].source
            storage_client = self._init_storage_client()
            table_detail = storage_client.tables.detail(table_id)

            columns = []

            if table_detail.get("isTyped", False) and table_detail.get("definition"):
                primary_keys = set(
                    table_detail["definition"].get("primaryKeysNames", [])
                )

                for column in table_detail["definition"]["columns"]:
                    col_name = column["name"]
                    col_def = column["definition"]

                    columns.append(
                        ColumnConfig(
                            source_name=col_name,
                            destination_name=col_name,
                            dtype=col_def.get("type", "STRING"),
                            pk=col_name in primary_keys,
                            nullable=col_def.get("nullable", True),
                            default_value=None,
                        ).model_dump()
                    )
            else:  # Non-typed table
                primary_keys = set(table_detail.get("primaryKey", []))

                for col_name in table_detail.get("columns", []):
                    columns.append(
                        ColumnConfig(
                            source_name=col_name,
                            destination_name=col_name,
                            dtype="STRING",
                            pk=col_name in primary_keys,
                            nullable=col_name not in primary_keys,
                            default_value=None,
                        ).model_dump()
                    )

        return {
            "type": "data",
            "data": {
                "destination": {
                    "table": self.params.destination.table,
                    "load_type": self.params.destination.load_type,
                    "columns": columns,
                    "preserve_insertion_order": self.params.destination.preserve_insertion_order,
                },
                "debug": self.params.debug,
            },
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
