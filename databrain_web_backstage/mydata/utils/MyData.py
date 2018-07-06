from datetime import datetime

from task.Task import Task
from mydata.models import Data, Field, DataImport
from setting import HDFS_MY_DATA_PATH


class MyData(object):

    class STATUS:

        INIT = "INIT"
        IMPORT = "IMPORT"
        READY = "READY"
        FAILED = "FAILED"

    class TYPE:

        DATABASE = "DATABASE"

    @classmethod
    def check_name_uniqueness(cls, user_id, name):
        """
        check whether the name of data has been used by other data
        :param user_id: data owner's id
        :param name: data's name
        :return: true if the name has not been used
        """
        data_with_same_name = Data.objects.only('id').filter(user_id=user_id, name = name)
        return len(data_with_same_name) == 0

    @classmethod
    def initialize(cls, user_id, name, fields):
        """
        initialize data
        :param user_id: data owner's id
        :param name: data's name
        :param fields: field description
        :return: None
        """
        model_data = Data(user_id=user_id, name=name, create_time=datetime.now(),status=cls.STATUS.INIT)
        model_data.save()
        model_fields = list()
        for field in fields:
            model_field = Field(data_id=model_data.id, name=field.field, type_l=field.sparkType)
            model_fields.append(model_field)
        Field.objects.bulk_create(model_fields)
        return model_data.id

    @classmethod
    def import_(cls, data_id, type_,*args, **kwargs):
        """
        data import
        including generate config, register a task, and fire a task
        :param data_id:  data id
        :param type_:  type of data import task, support database, hdfs, text-file
        :param detail: configuration detail
        :return: None
        """
        data_path = cls.my_data_path(data_id)
        config = cls.generate_config(data_path,*args,**kwargs)
        task_id = Task.register(Task.TYPE.DATA_IMPORT)
        DataImport(data_id= data_id, task_id= task_id, type_l= type_, config= config).save()
        Task.fire(task_id)


    @classmethod
    def generate_config(cls,data_path,*args, **kwargs):
        """
        generate config json
        :param detail: configuration detail
        :return: configuration json
        :rtype:
        """
        raise NotImplemented()

    @classmethod
    def my_data_path(cls, data_id):
        return HDFS_MY_DATA_PATH + ("%s" %data_id)
