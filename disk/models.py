from __future__ import unicode_literals
from django.db import models

# Create your models here.
class User(models.Model):
    # FileField：用于存储文件字符串，upload_to设置文件上传路径
    filename = models.FileField(upload_to = './upload/')

