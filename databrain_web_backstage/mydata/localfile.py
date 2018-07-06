import os
from datetime import datetime

from common import ERRORS
from common.UTIL import auto_param, login_required, md5, Response
from mydata.utils.localfile.LocalFileLifeCycle import STATUS
from setting import MY_DATA_LOCAL_FILE_TMP_DIR, MY_DATA_LOCAL_FILE_MAX_SIZE_IN_MB
from mydata.models import LocalFile


# create directory when not exits
if not os.path.exists(MY_DATA_LOCAL_FILE_TMP_DIR) or not os.path.isdir(MY_DATA_LOCAL_FILE_TMP_DIR):
    os.mkdir(MY_DATA_LOCAL_FILE_TMP_DIR)


MY_DATA_LOCAL_FILE_MAX_SIZE_IN_BYTE = MY_DATA_LOCAL_FILE_MAX_SIZE_IN_MB*1024*1024


@login_required
@auto_param(post_only=True)
def upload(request, file, user_pk):
    """
    file upload
    :param request:
    :param file:
    :param user_pk:
    :return:
    """
    if file.size > MY_DATA_LOCAL_FILE_MAX_SIZE_IN_BYTE:
        return Response.fail(ERRORS.MY_DATA_UPLOAD_SIZE_EXCEED)

    file_name = md5(user_pk, datetime.now())

    # saving the file
    file_saving_path = os.path.join(MY_DATA_LOCAL_FILE_TMP_DIR, file_name)
    with open(file_saving_path, 'wb') as destination:
        if file.multiple_chunks():
            for chunk in file.chunks():
                destination.write(chunk)
        else:
            destination.write(file.read())

    # record the data
    LocalFile(file_name=file_name, status=STATUS.UPLOADED).save()
    return Response.success(file_name)
