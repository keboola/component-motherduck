from enum import Enum
from typing import Optional

from keboola.component.exceptions import UserException
from pydantic import BaseModel, Field, ValidationError, computed_field


class LoadType(str, Enum):
    full_load = "full_load"
    incremental_load = "incremental_load"


class ColumnConfig(BaseModel):
    source_name: str
    destination_name: str
    dtype: str
    pk: bool
    nullable: bool
    default_value: Optional[str] = None


class Destination(BaseModel):
    table: str
    columns: list[ColumnConfig] = []
    load_type: LoadType = Field(default=LoadType.incremental_load)

    @computed_field
    def incremental(self) -> bool:
        return self.load_type in (LoadType.incremental_load)


class Configuration(BaseModel):
    token: str = Field(alias="#token", default=None)
    db: str = None
    db_schema: str = None
    destination: Destination = None
    debug: bool = False
    threads: int = 1
    max_memory: int = 256

    def __init__(self, **data):
        try:
            super().__init__(**data)
        except ValidationError as e:
            error_messages = [f"{err['loc'][0]}: {err['msg']}" for err in e.errors()]
            raise UserException(f"Validation Error: {', '.join(error_messages)}")
