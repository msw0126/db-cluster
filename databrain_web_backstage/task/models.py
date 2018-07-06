from django.db import models

from task.TASK_STATUS import TASK_STATUS


class Task(models.Model):

    type_l = models.CharField(max_length=20)
    record_time = models.DateTimeField()
    submit_time = models.DateTimeField(null=True)
    start_time = models.DateTimeField(null=True)
    end_time = models.DateTimeField(null=True)
    task_status = models.CharField(max_length=20, default=TASK_STATUS.PENDING)
    celery_id = models.CharField(max_length=100, null=True)
    error_code = models.CharField(max_length=30, null=True)
    application_id = models.CharField(max_length=50, null=True)
    tracking_url = models.CharField(max_length=200, null=True)
    detail = models.CharField(max_length=10000, null=True)
    has_log = models.BooleanField(default=False)
    relies = models.IntegerField(default=0)
    fire = models.BooleanField(default=False)
