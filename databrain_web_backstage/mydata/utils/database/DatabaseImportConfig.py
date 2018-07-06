from mydata.utils import DataType
from ..DataImportConfig import DataImportConfig


class DatabaseImportConfig(DataImportConfig):

    def __init__(self, dbType, url, table, user, password, schema, destPath):
        super().__init__(schema, destPath)
        self.dbType = dbType # type: str
        self.password = password # type: str
        self.table = table # type: str
        self.url = url # type: str
        self.user = user # type: str
