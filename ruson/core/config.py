from pydantic import BaseModel


class Config(BaseModel):
    connection_name: str = "default"
    database_uri: str
    database_name: str
