from datetime import datetime

from task.TASK_STATUS import TASK_STATUS
from .models import Task as TaskModel


class Task(object):

    class TYPE:

        DATA_IMPORT = "DATA_IMPORT"

    @classmethod
    def register(cls, type_, fire=False):
        """
        register a task
        :param type_: task type
        :param fire: execute immediately or not
        :return:  task id
        """
        registerd_task = TaskModel(type_l=type_, record_time=datetime.now(), fire = fire)
        registerd_task.save()
        return registerd_task.id

    @classmethod
    def fire(cls, task_id):
        """
        fire a task, execute immediately
        :param task_id: task id
        :return:
        """
        TaskModel.objects.filter(id=task_id).update(fire=True)
