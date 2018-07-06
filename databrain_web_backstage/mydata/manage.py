from common.UTIL import login_required, auto_param, NO_DETAIL_SUCCESS, page, model_transform, Response
from mydata.models import Data


# @login_required
@auto_param()
def data_list(request, user_pk, key_word=None, page_num:int=1, page_size:int=20):
    """
    list data
    :param request: request object
    :param user_pk: user id
    :param key_word: query key word of data name
    :param page_num:  page number
    :param page_size:  page size
    :return: data
    """
    query_param = dict(user_id=user_pk)
    if key_word is not None:
        query_param.setdefault('name__icontains','key_word')
    data_objects = page(Data, model_transform,page_num, query_param, page_size)
    return Response.success(data_objects)


# @login_required
@auto_param()
def data_delete(request, user_pk, data_id):
    # delete file on HDFS, and database information
    pass


# @login_required
@auto_param()
def data_batch_delete(request, user_pk, data_ids):
    pass

