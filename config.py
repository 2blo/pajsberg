from pydantic import BaseModel


class Config(BaseModel):
    warehouse: str
    catalog_file: str
    namespace: str
    catalog_name: str
