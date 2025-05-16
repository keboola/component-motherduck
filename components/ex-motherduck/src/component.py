import logging
import os
import time
from collections import OrderedDict

import duckdb
import polars
from keboola.component.base import ComponentBase, sync_action
from keboola.component.dao import ColumnDefinition, BaseType, SupportedDataTypes
from keboola.component.exceptions import UserException
from keboola.component.sync_actions import SelectElement, ValidationResult, MessageType

from configuration import Configuration

DUCK_DB_DIR = os.path.join(os.environ.get("TMPDIR", "/tmp"), "duckdb")


class Component(ComponentBase):
    def __init__(self):
        super().__init__()
        self.params = Configuration(**self.configuration.parameters)
        self.db = self.init_connection()

    def run(self):
        """
        Main execution code
        """

        start_time = time.time()

        table_path = f"{self.params.db}.{self.params.db_schema}.{self.params.data_selection.table}"

        query = self.get_query(table_path)

        table_meta = self.db.execute(f"DESCRIBE {query};").fetchall()
        schema = OrderedDict(
            {
                c[0]: ColumnDefinition(
                    data_types=BaseType(dtype=self.convert_base_types(c[1])),
                    primary_key=c[3] == "PRI" if not self.params.destination.primary_key else False,
                )
                for c in table_meta
            }  # c[0] is the column name, c[1] is the data type, c[3] is the primary key
        )

        table_name = self.params.destination.table_name or self.params.data_selection.table

        out_table = self.create_out_table_definition(
            f"{table_name}.csv",
            schema=schema,
            primary_key=self.params.destination.primary_key,
            incremental=self.params.destination.incremental,
            has_header=True,
        )

        try:
            q = f"COPY ({query}) TO '{out_table.full_path}' (HEADER, DELIMITER ',', FORCE_QUOTE *)"
            logging.debug(f"Running query: {q}; ")
            start = time.time()
            self.db.execute(q)
            logging.debug(f"Query finished successfully in {time.time() - start:.2f} seconds")
        finally:
            self.db.close()

        self.write_manifest(out_table)

        logging.debug(f"Execution time: {time.time() - start_time:.2f} seconds")

    def init_connection(self):
        os.makedirs(DUCK_DB_DIR, exist_ok=True)

        config = {
            "temp_directory": DUCK_DB_DIR,
            "extension_directory": os.path.join(DUCK_DB_DIR, "extensions"),
            "threads": self.params.threads,
            "max_memory": f"{self.params.max_memory}MB",
            "motherduck_token": self.params.token
        }

        conn = duckdb.connect(database="md:", config=config)

        if not self.params.destination.preserve_insertion_order:
            conn.execute("SET preserve_insertion_order = false;")

        return conn

    def get_query(self, table_path: str) -> str:
        match self.params.data_selection.mode:
            case "custom_query":
                query = self.params.data_selection.query.lower().replace("from in_table ", f"FROM {table_path}")
            case "select_columns":
                query = f"SELECT {", ".join(self.params.data_selection.columns)} FROM {table_path}"
            case "all_data":
                query = f"SELECT * FROM {table_path}"
            case _:
                raise UserException("Invalid data selection mode")

        return query

    @staticmethod
    def convert_base_types(dtype: str) -> SupportedDataTypes:
        if dtype in [
            "TINYINT",
            "SMALLINT",
            "INTEGER",
            "BIGINT",
            "HUGEINT",
            "UTINYINT",
            "USMALLINT",
            "UINTEGER",
            "UBIGINT",
            "UHUGEINT",
        ]:
            return SupportedDataTypes.INTEGER
        elif dtype in ["REAL", "DECIMAL"]:
            return SupportedDataTypes.NUMERIC
        elif dtype == "DOUBLE":
            return SupportedDataTypes.FLOAT
        elif dtype == "BOOLEAN":
            return SupportedDataTypes.BOOLEAN
        elif dtype in ["TIMESTAMP", "TIMESTAMP WITH TIME ZONE"]:
            return SupportedDataTypes.TIMESTAMP
        elif dtype == "DATE":
            return SupportedDataTypes.DATE
        else:
            return SupportedDataTypes.STRING

    @staticmethod
    def to_markdown(text: str) -> str:
        polars.Config.set_tbl_formatting("ASCII_MARKDOWN")
        polars.Config.set_tbl_hide_dataframe_shape(True)
        formatted_output = str(text)
        return formatted_output

    @sync_action("testConnection")
    def test_connection(self):
        pass  # just init connection

    @sync_action("list_databases")
    def list_databases(self):
        databases = self.db.execute("SHOW ALL DATABASES").fetchall()
        return [SelectElement(d[0]) for d in databases]

    @sync_action("list_schemas")
    def list_schemas(self):
        schemas = self.db.execute(f"""
        SELECT schema_name
        FROM information_schema.schemata
        WHERE catalog_name = '{self.params.db}'
        """).fetchall()

        return [SelectElement(s[0]) for s in schemas]

    @sync_action("list_tables")
    def list_tables(self):
        tables = self.db.execute(f"""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_catalog = '{self.params.db}'
        AND table_schema = '{self.params.db_schema}'
        """).fetchall()

        return [SelectElement(t[0]) for t in tables]

    @sync_action("list_columns")
    def list_columns(self):
        table_path = f"{self.params.db}.{self.params.db_schema}.{self.params.data_selection.table}"
        out = self.db.execute(f"DESCRIBE {table_path};").fetchall()
        column_names = [SelectElement(c[0], f"{c[0]} ({c[1]})") for c in out]
        return column_names

    @sync_action("table_preview")
    def table_preview(self):
        table_path = f"{self.params.db}.{self.params.db_schema}.{self.params.data_selection.table}"
        out = self.db.execute(f"""
                SELECT *
                FROM {table_path}
                LIMIT 10;
                """).pl()

        formatted_output = self.to_markdown(out)
        return ValidationResult(formatted_output, MessageType.SUCCESS)

    @sync_action("query_preview")
    def query_preview(self):
        table_path = f"{self.params.db}.{self.params.db_schema}.{self.params.data_selection.table}"
        query = self.params.data_selection.query % table_path

        if "limit" not in query.lower():
            if ";" in query:
                query = query.replace(";", " LIMIT 10;")
            else:
                query = f"{query} LIMIT 10;"

        out = self.db.execute(query).pl()
        formatted_output = self.to_markdown(out)
        return ValidationResult(formatted_output, MessageType.SUCCESS)


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
