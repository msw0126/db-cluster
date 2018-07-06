from common.UTIL import to_json
from ..MyData import MyData
from .DatabaseImportConfig import DatabaseImportConfig


class DataBaseImport(MyData):

    URL_MAPPING = dict(
        oracle = "jdbc:oracle:thin:@%s:%d:%s",
        mysql = "jdbc:mysql://%s:%d/%s"
    )

    @classmethod
    def generate_config(cls,dest_path, db_type, ip, port, db, user, password, table, fields):
        """
        generate config json
        :return: configuration file
        """
        url = cls.URL_MAPPING[db_type] %(ip, port,db)
        config = DatabaseImportConfig(db_type, url,table, user, password, fields, dest_path)
        return to_json(config)
