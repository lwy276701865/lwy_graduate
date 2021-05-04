# -*- coding:utf-8 -*-
import pymysql
import csv
from pymysql import connect
from django.shortcuts import render
from django import forms
from django.http import HttpResponse,HttpResponseRedirect
from disk.models import User
import os
from os import path
import numpy as np
import pandas as pd
# Create your views here.
class UserForm(forms.Form):
    filename = forms.FileField(label='请选择文件')

def index01(request):
    if request.method == 'GET':
        return render(request,'disk/index.html')
    elif request.method == 'POST':
        content =request.FILES.get("upload", None)
        if not content:
            return render(request,  'disk/index.html', {'message': '没有上传内容'})
        position = os.path.join('./upload',content.name)
        #获取上传文件的文件名，并将其存储到指定位置
        storage = open(position,'wb+')       #打开存储文件
        for chunk in content.chunks():       #分块写入文件
            storage.write(chunk)
        storage.close()
        file_path = position.replace('\\','\\'+'\\')
        table_name =content.name.replace('.csv','')
        con = pymysql.connect(user="root",
                              passwd="lwydecd+20",
                              db="test",
                              host="localhost",
                              local_infile=1)
        cur = con.cursor()
        #打开csv文件
        file = open(file_path, 'r',encoding='utf8')
        #读取csv文件第一行字段名，创建表
        reader = file.readline()
        devide = reader.split(',')
        # 去除最后的换行符
        devide[-1] = devide[-1].rstrip('\n')
        print("devide......0022111")
        print(devide)
        column = ''
        for a in devide:
            column = column + '`' +a+ '`' + ' varchar(255),'
            print("coulmn225151151")
            print(column)
        col = column.rstrip(',')
        #编写sql，create_sql负责创建表，data_sql负责导入数据
        create_table_sql = 'create table if not exists {} ({}) DEFAULT CHARSET=utf8'.format("`"+table_name+"`", col)
        data_sql = 'LOAD DATA LOCAL INFILE \'' + file_path + '\'REPLACE INTO TABLE ' + table_name + \
                   ' CHARACTER SET UTF8 FIELDS TERMINATED BY \',\' ENCLOSED BY \'\"\' LINES TERMINATED BY \'\n\' IGNORE 1 LINES;'

        print(create_table_sql)
        #使用数据库
        cur.execute('use %s' % 'test')
        #设置编码格式
        cur.execute('SET NAMES utf8;')
        cur.execute('SET character_set_connection=utf8;')
        #执行create_sql，创建表
        cur.execute(create_table_sql)
        #执行data_sql，导入数据
        cur.execute(data_sql)
        con.commit()
        #关闭连接
        con.close()
        cur.close()
        return render(request,  'disk/index.html', {'message': '上传成功'})      #返回客户端信息
    else:
        return HttpResponseRedirect("不支持的请求方法")
def index(request):
    if request.method == 'GET':
        return render(request,'disk/index.html')
    elif request.method == 'POST':
        content =request.FILES.get("upload", None)
        if not content:
            return render(request,  'disk/index.html', {'message': '没有上传内容'})
        position = os.path.join('./upload',content.name)
        #获取上传文件的文件名，并将其存储到指定位置
        storage = open(position,'wb+')       #打开存储文件
        for chunk in content.chunks():       #分块写入文件
            storage.write(chunk)
        storage.close()
        file_path = position
        hostname = '127.0.0.1'
        port = 3306
        user = 'root'
        passwd = 'lwydecd+20'
        db = 'test'
        M = CsvToMysql(hostname=hostname, port=port, user=user, passwd=passwd, db=db)
        M.read_csv(file_path)
        return render(request,  'disk/index.html', {'message': '上传成功'})      #返回客户端信息
    else:
        return HttpResponseRedirect("不支持的请求方法")
class CsvToMysql(object):
    def __init__(self, hostname, port, user, passwd, db):
        self.dbname = db
        self.conn = connect(host=hostname, port=port, user=user, passwd=passwd, db=db)
        self.cursor = self.conn.cursor()

    # 读取csv文件
    def read_csv(self,filename):
        df = pd.read_csv(filename, keep_default_na=False, encoding='utf-8')
        table_name = '`'+os.path.split(filename)[-1].split('.')[0] + '`'
        print("111111111111111111111111111111111111111111111111")
        print(os.path.split(filename))
        print(os.path.split(filename)[-1])
        print(os.path.split(filename)[-1].split('.'))
        print(os.path.split(filename)[-1].split('.')[0])
        self.csv2mysql(db_name=self.dbname,table_name=table_name, df=df )

    # pandas的数据类型和MySQL是不通用的，需要进行类型转换。字段名可能含有非法字符，需要反引号。
    def make_table_sql(self,df):
        #将csv中的字段类型转换成mysql中的字段类型
        columns = df.columns.tolist()
        make_table = []
        make_field = []
        for col in columns:
            item1 = '`'+col+'`'
            if 'int' in str(df[col].dtype):
                char = item1 + ' INT'
            elif 'float' in str(df[col].dtype):
                char = item1 + ' FLOAT'
            elif 'object' in str(df[col].dtype):
                char = item1 + ' VARCHAR(255)'
            elif 'datetime' in str(df[col].dtype):
                char = item1 + ' DATETIME'
            else:
                char = item1 + ' VARCHAR(255)'
            make_table.append(char)
            make_field.append(item1)
        return ','.join(make_table), ','.join(make_field)


    def csv2mysql(self,db_name,table_name,df):
        field1, field2 = self.make_table_sql(df)
        print("create table {} ( {})".format(table_name,field1))
        self.cursor.execute('drop table if exists {}'.format(table_name))
        self.cursor.execute("create table {} ({})".format(table_name, field1))
        values = df.values.tolist()
        s = ','.join(['%s' for _ in range(len(df.columns))])
        try:
            print(len(values[0]),len(s.split(',')))
            print ('insert into {}({}) values ({})'.format(table_name, field2, s), values[0])
            self.cursor.executemany('insert into {}({}) values ({})'.format(table_name, field2, s), values)
        except Exception as e:
            print (e.message)
        finally:
            self.conn.commit()

