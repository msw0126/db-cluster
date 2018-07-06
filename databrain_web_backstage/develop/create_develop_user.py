import sys
import os
import django

sys.path.append(os.path.dirname(os.path.realpath(sys.argv[0])))
os.environ['DJANGO_SETTINGS_MODULE'] = 'databrain_python.settings'
django.setup()

from django.contrib.auth.models import User

user = User.objects.create_user('admin', password='123456')
user.save()
