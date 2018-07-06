# -*- coding:utf-8 -*-

import os, subprocess, sys


SPARK_PATH = "C:\\Spark\\spark-2.1.0-bin-hadoop2.7\\bin\\spark-submit"
HADOOP_CONFIG = "C:\\Spark\\hadoop_config"
HADOOP_USER_NAME = "hdfs"
SPARK_CLASSPATH = "C:\\Spark\\spark-2.1.0-bin-hadoop2.7\\jars"


os.environ.setdefault("HADOOP_CONF_DIR", HADOOP_CONFIG)
os.environ.setdefault("HADOOP_USER_NAME", HADOOP_USER_NAME)
os.environ.setdefault("YARN_CONF_DIR", HADOOP_CONFIG)
os.environ.setdefault("SPARK_CLASSPATH", SPARK_CLASSPATH)

command = [
        SPARK_PATH,
        "--master", "yarn",
        "--deploy-mode", "cluster",
        "--name", "test",
        "--files", "..\\tdir\\data_split_config.json",
        "--py-files", "..\\tdir\\my_data_split.zip",
        "--driver-memory", "1G",
        "--num-executors", "1",
        "--executor-memory", "1G",
        "F:\\work\\databrain-cluster\\component-dev\\my_data_split\\tdir\\datasplit_run.py", "data_split_config.json"
    ]
print(" ".join(command))
print(os.path.dirname(os.path.realpath(sys.argv[0])))
try:
    p = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, cwd=os.path.dirname(os.path.realpath(sys.argv[0])))
    application_id = None
    tracking_url = None
    while p.poll() is None:
        line = p.stderr.readline().decode('utf-8', 'ignore').strip()
        print(line)
        if len(line) > 0 and (application_id is None or tracking_url is None):
            assert isinstance(line, str)
            if line.startswith("tracking URL:"):
                tracking_url = line.replace("tracking URL:", "").strip()
                print(tracking_url)
            elif "Submitted application" in line:
                application_id = line.split("Submitted application")[1].strip()
                print(application_id)
except Exception as e:
    # print(str(e).decode('cp936').encode('utf-8'))
    # raise e
    print e


# set HADOOP_CONF_DIR=C:\work\soft\hadoop_config
# set YARN_CONF_DIR=C:\work\soft\hadoop_config
# set HADOOP_USER_NAME=hdfs
# set SPARK_CLASSPATH=C:\work\soft\spark22\jars
# C:\work\soft\spark22\bin\spark-submit --master yarn-cluster --name test_ttt --driver-memory 1G --num-executors 1 --executor-memory 1G ttt.py
