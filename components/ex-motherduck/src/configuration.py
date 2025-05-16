from enum import Enum

from keboola.component.exceptions import UserException
from pydantic import BaseModel, Field, ValidationError, computed_field


class DataSelectionMode(str, Enum):
    all_data = "all_data"
    select_columns = "select_columns"
    custom_query = "custom_query"


class LoadType(str, Enum):
    full_load = "full_load"
    incremental_load = "incremental_load"


class Destination(BaseModel):
    table_name: str = Field(default=None)
    load_type: LoadType = Field(default=LoadType.incremental_load)
    primary_key: list[str] = Field(default_factory=list)
    preserve_insertion_order: bool = True

    @computed_field
    @property
    def incremental(self) -> bool:
        return self.load_type in (LoadType.incremental_load)


class DataSelection(BaseModel):
    table: str = Field(default=None)
    mode: DataSelectionMode = Field(default=DataSelectionMode.all_data)
    columns: list[str] = Field(default_factory=list)
    query: str = Field(default=None)


class Configuration(BaseModel):
    token: str = Field(alias="#token", default=None)
    db: str = None
    db_schema: str = None
    destination: Destination = Field(default_factory=Destination)
    data_selection: DataSelection = None
    debug: bool = False
    threads: int = 1
    max_memory: int = 256

    def __init__(self, **data):
        try:
            super().__init__(**data)
        except ValidationError as e:
            error_messages = [f"{err['loc'][0]}: {err['msg']}" for err in e.errors()]
            raise UserException(f"Validation Error: {', '.join(error_messages)}")
