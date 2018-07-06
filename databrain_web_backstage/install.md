### python-packages
- django==1.11.7
- pymysql==0.7.11
- django-cors-headers==2.1.0
- celery==3.1.25
- deepdiff==3.3.0
- py4j==0.10.6
- django-celery==3.2.2




### install mysql in local machine with root and password 123456
### create database databrain with encoding utf-8
### init table
```bash
python manage.py makemigrations
python manage.py migrate
python manage.py makemigrations task
python manage.py migrate task
python manage.py makemigrations mydata
python manage.py migrate mydata
```

### create user by running `develop/create_develop_user.py`

### runserver
python manage.py runserver 0.0.0.0:8022


