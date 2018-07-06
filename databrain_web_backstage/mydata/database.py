from typing import List

from django.http import HttpResponse

from common.UTIL import auto_param, py4j, Response, login_required, NO_DETAIL_SUCCESS
from common.ERRORS import DATABASE_CONNECTION_ERROR, DATA_NAME_HAS_BEEN_USED
from py4j.java_gateway import JavaGateway

# py4j gateway and corresponding java function
from mydata.utils.database.DatabaseImport import DataBaseImport
from .utils.DataType import DataType

gate_way = JavaGateway()
JavaMyData = gate_way.jvm.com.taodatarobot.mydata.Database
java_table_list = JavaMyData.listDatabaseTable
java_table_query = JavaMyData.queryDatabaseTable
java_table_view = JavaMyData.viewDatabaseTable
java_preview = JavaMyData.preview


def column_description_converter(result):
    """
    convert java ColumnDescription to python list
    :param result: Java Array of ColumnDescription
    :return: Python Column Description, contains column name, corresponding spark type, sample data
    """
    preview = list()
    for column_description in result:
        column_name = column_description.getName()
        sample_data = column_description.getSampleData()
        sample_data = list(sample_data)
        data_type = column_description.getSparkDataType()
        preview.append(dict(
            name=column_name,
            sample_data=sample_data,
            data_type=data_type
        ))
    return preview


def py4j_query(java_function, converter, *args):
    """
    execute java function through py4j
    1. py4j connection error, return HttpResponse object showing py4j connection error
    2. database connection error, return HttpResponse object showing database connection err
    3. successfully execute java function , convert the java object to python object through converter function, return HttpResponse object containing result
    :param java_function: corresponding java function
    :param converter: converter which convert java object to python object
    :param args: other argument needed by java function
    :return: HttpResponse
    """
    try:
        result = py4j(java_function, *args)
        # error connecting to py4j
        if isinstance(result, HttpResponse):
            return result
        return Response.success(converter(result))
    except Exception as e:
        error_msg = e.java_exception.getMessage()
        return Response.fail(DATABASE_CONNECTION_ERROR, error_msg)


# @login_required
@auto_param()
def table_list(request, db_type, ip, port: int, db, user, password, query_str: str=None):
    """
    list table of database
    :param request: request object
    :param db_type: database type, only oracle and mysql supported
    :param ip: database server ip
    :param port: database server port
    :param db: database name
    :param user: user name
    :param password: password
    :return: HttpResponse
    """
    param = list()
    if query_str is None or query_str.strip() == '':
        # 参数为空，调用listTable，获取所有的表
        # func = 'listTable'
        java_function = java_table_list
        converter = list
        return py4j_query( java_function, converter, db_type, ip, port, db, user, password )
    else:
        # 参数非空，模糊匹配表
        java_function = java_table_query
        converter = list
        return py4j_query( java_function, converter, db_type, ip, port, db, user, password, query_str )


# @auto_param
# def table_view(request, db_type, ip, port: int, db, user, password, table, n: int):
#     result = py4j_common_db_util('viewDatabaseTable', db_type, ip, port, db, user, password, table, n)
#     if isinstance(result, HttpResponse):
#         return result
#     return HttpResponse( Response.success([dict(name=k, value=list(v)) for k, v in result.items()]).to_json())


# @login_required
@auto_param()
def data_mapping(request, db_type, ip, port: int, db, user, password, table, n: int):
    """
    preview table structure, contains column name, sample data, corresponding spark data type
    :param request: request object
    :param db_type: database type
    :param ip: database server ip
    :param port: database server port
    :param db: database name
    :param user: user name
    :param password: password
    :param table: table selected
    :param n: number of record to preview
    :return: HttpResponse
    """
    java_function = java_preview
    converter = column_description_converter
    return py4j_query(java_function, converter, db_type, ip, port, db, user, password, table, n)


# @login_required
@auto_param()
def data_import(request, db_type, ip, port: int, db, user, password, table, data_name, fields: List[DataType]):
    """
    import database data to hdfs
    1. record information of the data, including name, fields
    2. submit a task to celery, record the information of task
    :param request: request object
    :param db_type: database type
    :param ip: database server ip
    :param port: database server port
    :param db: database name
    :param user: user name
    :param password: password
    :param table: name of table need import
    :param data_name: data name
    :param fields: filed description
    :return:
    """
    user_id = request.user.id
    unique = DataBaseImport.check_name_uniqueness(user_id, data_name)
    if not unique:
        return Response.fail(DATA_NAME_HAS_BEEN_USED)
    data_id = DataBaseImport.initialize(user_id, data_name, fields)
    DataBaseImport.import_(data_id, DataBaseImport.TYPE.DATABASE, db_type, ip, port, db, user, password, table, fields)
    return NO_DETAIL_SUCCESS
