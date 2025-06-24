import logging
import time

from keboola.component.base import ComponentBase, sync_action
from keboola.component.exceptions import UserException
from keboola.component.sync_actions import SelectElement

from client.duck import DuckConnection
from client.storage_api import SAPIClient
from configuration import ColumnConfig, Configuration


class Component(ComponentBase):
    def __init__(self):
        super().__init__()
        self.params = Configuration(**self.configuration.parameters)
        self.db = DuckConnection(self.params)

    def run(self):
        """
        Main execution code
        """

        start_time = time.time()

        in_table_definition = self._get_in_table()
        self.db.upload_table(
            in_table_definition=in_table_definition,
            destination=f"{self.params.db}.{self.params.db_schema}.{self.params.destination.table}",
        )

        logging.debug(f"Execution time: {time.time() - start_time:.2f} seconds")

    def _get_in_table(self):
        in_tables = self.get_input_tables_definitions()
        if len(in_tables) != 1:
            raise UserException(f"Exactly one input table is expected. Found: {[t.destination for t in in_tables]}")
        return in_tables[0]

    @staticmethod
    def _map_to_duckdb_type(keboola_type: str) -> str:
        """
        Maps Keboola data types to DuckDB data types.

        Args:
            keboola_type: The Keboola data type

        Returns:
            str: Corresponding DuckDB data type
        """
        type_mapping = {
            "STRING": "VARCHAR",
            "INTEGER": "INTEGER",
            "NUMERIC": "DECIMAL",
            "FLOAT": "FLOAT",
            "BOOLEAN": "BOOLEAN",
            "DATE": "DATE",
            "TIMESTAMP": "TIMESTAMP",
        }
        return type_mapping.get(keboola_type, keboola_type)

    def _get_sapi_column_definition(self):
        table_id = self.configuration.tables_input_mapping[0].source
        storage_client = SAPIClient(self.environment_variables.url, self.environment_variables.token)
        table_detail = storage_client.get_table_detail(table_id)
        columns = []
        if table_detail.get("isTyped") and table_detail.get("definition"):
            primary_keys = set(table_detail["definition"].get("primaryKeysNames", []))
            columns_to_process = [
                {
                    "name": column["name"],
                    "dtype": self._map_to_duckdb_type(column["definition"].get("type", "VARCHAR")),
                    "nullable": column["definition"].get("nullable", True),
                }
                for column in table_detail["definition"]["columns"]
            ]

        else:  # non-typed table
            primary_keys = set(table_detail.get("primaryKey", []))
            columns_to_process = [
                {
                    "name": col_name,
                    "dtype": "VARCHAR",
                    "nullable": col_name not in primary_keys,
                }
                for col_name in table_detail.get("columns", [])
            ]

        # Create column configs for all columns
        for col_info in columns_to_process:
            col_name = col_info["name"]
            columns.append(
                ColumnConfig(
                    source_name=col_name,
                    destination_name=col_name,
                    dtype=col_info["dtype"],
                    pk=col_name in primary_keys,
                    nullable=col_info["nullable"],
                    default_value=None,
                ).model_dump()
            )
        return columns

    @sync_action("testConnection")
    def test_connection(self):
        pass  # just init connection

    @sync_action("list_databases")
    def list_databases(self):
        databases = self.db.connection.execute("SHOW ALL DATABASES").fetchall()
        return [SelectElement(d[0]) for d in databases]

    @sync_action("list_schemas")
    def list_schemas(self):
        schemas = self.db.connection.execute(f"""
        SELECT schema_name
        FROM information_schema.schemata
        WHERE catalog_name = '{self.params.db}'
        """).fetchall()

        return [SelectElement(s[0]) for s in schemas]

    @sync_action("list_tables")
    def list_tables(self):
        tables = self.db.connection.execute(f"""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_catalog = '{self.params.db}'
        AND table_schema = '{self.params.db_schema}'
        """).fetchall()

        return [SelectElement(t[0]) for t in tables]

    @sync_action("return_columns_data")
    def return_columns_data(self):
        if self.params.destination.columns:
            columns = []
            for col in self.params.destination.columns:
                col_data = col.model_dump()
                col_data["dtype"] = self._map_to_duckdb_type(col_data["dtype"])
                columns.append(col_data)
        else:
            if len(self.configuration.tables_input_mapping) != 1:
                raise UserException(
                    "Exactly one input table is expected. Found: "
                    f"{[t.destination for t in self.configuration.tables_input_mapping]}"
                )

            columns = self._get_sapi_column_definition()

        return {
            "type": "data",
            "data": {
                "destination": {
                    "table": self.params.destination.table,
                    "load_type": self.params.destination.load_type,
                    "columns": columns,
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
