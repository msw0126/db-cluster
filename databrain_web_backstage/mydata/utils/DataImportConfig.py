from mydata.utils import DataType


class DataImportConfig:

    def __init__(self, schema, destPath):
        self.destPath = destPath # type: str
        self.schema = schema # type: list[DataType]
