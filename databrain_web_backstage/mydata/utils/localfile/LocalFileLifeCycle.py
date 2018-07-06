from datetime import datetime, timedelta
from django.db.models import Q
from setting import MY_DATA_LOCAL_FILE_EXPIRED_DAYS
from mydata.models import LocalFile


class STATUS:

    UPLOADED = "UPLOADED"
    LOADING = "LOADING"
    ABANDONED = "ABANDONED"


def clear_file():
    """
    clear files
    1. status is abandoned, which could be loaded into HDFS or failed
    2. status is uploaded, and expired
    :return: None
    """
    LocalFile.objects(Q(status=STATUS.ABANDONED)|
                      Q(status=STATUS.UPLOADED,
                        upload_time_lte=datetime.now()-timedelta(days=MY_DATA_LOCAL_FILE_EXPIRED_DAYS)))\
        .delete()

