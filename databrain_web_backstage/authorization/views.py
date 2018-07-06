from common.UTIL import auto_param, Response, login_required,NO_DETAIL_SUCCESS
from django.contrib import auth
from common.ERRORS import LOGIN_ERROR


@auto_param()
def login(request, user, password):
    user = auth.authenticate(username=user, password=password)
    if user is not None:
        auth.login(request, user)
        return NO_DETAIL_SUCCESS
    else:
        return Response.fail(LOGIN_ERROR)


@login_required
@auto_param()
def logout(request):
    auth.logout(request)
    return NO_DETAIL_SUCCESS
