from django.db import models
from django.utils.timezone import now


class Data(models.Model):

    user_id = models.IntegerField()
    name = models.CharField(max_length=50)
    create_time = models.DateTimeField() # create time
    latest_ref_time = models.DateTimeField(null=True) # latest time referenced in project
    status = models.CharField(max_length=20) # data status: init->import->ready or fail
    data_size = models.IntegerField(null=True)
    num_column = models.SmallIntegerField(null=True)
    num_row = models.IntegerField(null=True)


class Field(models.Model):

    data_id = models.IntegerField()
    name = models.CharField(max_length=300)
    type_l = models.CharField(max_length=20)


class DataImport(models.Model):

    data_id = models.IntegerField()
    task_id = models.IntegerField()
    type_l = models.CharField(max_length=20)
    config = models.TextField()


class LocalFile(models.Model):

    file_name = models.CharField(max_length=50)
    status = models.CharField(max_length=10)  # status
    upload_time = models.DateTimeField(default=now)  # update time
