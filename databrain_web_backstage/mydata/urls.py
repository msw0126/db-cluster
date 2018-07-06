from django.conf.urls import url
from . import database, localfile, manage

urlpatterns = [
    url(r'^database/table_list$', database.table_list),
    url(r'^database/data_preview$', database.data_mapping),
    # url(r'^database/table_view$', database.table_view),
    url(r'^database/data_mapping$', database.data_mapping),
    url(r'^database/data_import$', database.data_import),
    url(r'^localfile/upload$', localfile.upload),
    url(r'^manage/data_list$', manage.data_list)
]
