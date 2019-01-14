from django.contrib import admin
from django.contrib.auth.models import *


# Register your models here.
admin.site.unregister(User)
admin.site.unregister(Group)

