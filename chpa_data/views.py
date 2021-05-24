import json
from django.http import HttpResponseRedirect
import os
from django.shortcuts import render
from django.http import HttpResponse
from pymysql import connect
from sqlalchemy import create_engine
import sqlalchemy as sa
import pandas as pd
import numpy as np
import six
import codecs
from .charts import *
try:
    from io import BytesIO as IO # for modern python
except ImportError:
    from io import StringIO as IO # for legacy python
import datetime
import xlsxwriter
import io
from django.contrib.auth.decorators import login_required
ENGINE = create_engine('mysql+pymysql://root:lwydecd+20@localhost:3306/test') #创建数据库连接引擎
# 根据用户传进来的数据创建的表
DB_TABLE=''
column=''
index=''
value=''
aggfunc=''
uploadfilename=''

# 此函数根据用户导入的文件来动态初始化界面
# 1.获取用户导入的文件
# 2.将文件以utf-8的编码形式写入新文件
# 3.以新文件来生成对应的mysql数据库，并将相关数据传至前端，初始化界面
@login_required
def index(request):
    if request.method == 'GET':
        return render(request, 'chpa_data/display.html')
    elif request.method == 'POST':
        content =request.FILES.get("upload", None)
        if not content:
            return render(request,  'chpa_data/display.html', {'message': '没有上传内容','metadata':'请上传文件'})
        position = os.path.join('./upload',content.name)
        global uploadfilename
        uploadfilename=content.name
        newfile=position[0:position.rfind('.')]+'toUTF-8.csv'
        #获取上传文件的文件名，并将其存储到指定位置
        # wb+:以二进制格式打开一个文件用于读写。如果该文件已存在则打开文件，并从开头开始编辑，即原有内容会被删除。
        # 如果该文件不存在，创建新文件。一般用于非文本文件如图片等。
        storage = open(position,'wb+')       #打开存储文件
        for chunk in content.chunks():       #分块写入文件
            storage.write(chunk)
        storage.close()
        #rb+:以二进制格式打开一个文件用于读写。文件指针将会放在文件的开头。一般用于非文本文件如图片等。
        f=open(position,'rb+')
        content_type=f.read()#读取文件内容，content_type为bytes类型，而非string类型

        # 获取文件编码方式
        source_encoding=get_file_code(content_type)

        # r:以只读方式打开文件。文件的指针将会放在文件的开头。这是默认模式。
        with codecs.open(position, "r",source_encoding) as f:
            newcontent=f.read()
        # wb:以二进制格式打开一个文件只用于写入。如果该文件已存在则打开文件，并从开头开始编辑，即原有内容会被删除。
        # 如果该文件不存在，创建新文件。一般用于非文本文件如图片等。
        with codecs.open(newfile, "wb") as f:
            f.write(newcontent.encode(encoding='utf-8', errors="ignore"))
        file_path = newfile

        global DB_TABLE
        # 根据用户导入的文件名选择最后一个X.csv的X为表名,并加上``符号
        DB_TABLE = '`'+os.path.split(file_path)[-1].split('.')[0] + '`'
        hostname = '127.0.0.1'
        port = 3306
        user = 'root'
        passwd = 'lwydecd+20'
        db = 'test'

        M = CsvToMysql(hostname=hostname, port=port, user=user, passwd=passwd, db=db)
        metadata=M.read_csv(file_path)
        if metadata=='null':
            return render(request,  'chpa_data/display.html', {'message': uploadfilename+'为空文件！','metadata':'您上传的文件为空文件，请重新上传'})
        sql='select column_name,data_type from information_schema.columns where table_name={} '.format(DB_TABLE.replace('`',"'"))
        # 获取表的字段名及类型
        df=pd.read_sql_query(sql,ENGINE)
        print("获取表的字段及其数据类型：")
        print(df)

        # 初始化前端选项
        mselect_dict,mselect_dict_value=init_html_form(df)

        # 初始化方法选择框
        aggfunc_select={
            '求和':'sum',
            '统计个数':'count',
            '求平均值':'mean',
            '求标准差':'std',
            '求方差':'var',
            '求中位数':'median'
        }
        context = {
            'mselect_dict':mselect_dict, #index,column
            'mselect_dict_value':mselect_dict_value, #value
            'message': uploadfilename+'已上传',
            'aggfunc_select':aggfunc_select, #运算函数
            'metadata':metadata #元数据信息或者提示空文件
        }
        return render(request,  'chpa_data/display.html',context)      #返回客户端信息
    else:
        return HttpResponseRedirect("不支持的请求方法")
# 此函数删除字符串两边的反引号``
def delesig(str):
    if str[0]=='`':
        str=str[1:][:-1]
    else:
        str=str
    return str
# 此函数根据form_dict数据做对应的处理（处理为原数据或者透视数据表）
def get_df(form_dict, is_pivoted=True):
    sql = sqlparse(form_dict)  # sql拼接
    get_originData_sql='select * from %s' %(DB_TABLE)
    df = pd.read_sql_query(sa.text(sql), ENGINE)  # 将sql语句结果读取至Pandas Dataframe
    originData_df=pd.read_sql_query(sa.text(get_originData_sql), ENGINE)
    print("构造出来的sql语句为"+sql)

    # 前端维度的选择用在透视函数的参数，数据筛选的选择用于生成df
    if is_pivoted is True:
        dimension_selected = form_dict['DIMENSION_select'][0]
        index_selected = form_dict['INDEX_select'][0]
        value_selected = form_dict['VALUE_select'][0]
        aggfunc_selected=form_dict['AGGFUNC_select'][0]
        global column
        column=delesig(dimension_selected)
        global index
        index=delesig(index_selected)
        global value
        value=delesig(value_selected)
        global aggfunc
        aggfunc=delesig(aggfunc_selected)
        pivoted = pd.pivot_table(df,
                                 values=value,
                                 index=index,
                                 columns=column,
                                 aggfunc=aggfunc,
                                 fill_value=0)
        return pivoted
    else:
        return originData_df
@login_required
# query函数在前端选择了筛选条件之后通过前端传递过来的值进行分析，并返回json格式的结果
# 1.解析前端参数到理想格式
# 2.根据前端参数数据拼接SQL并用Pandas读取
# 3.Pandas读取数据后，将前端选择的DIMENSION作为pivot_table方法的column参数
# 4.返回Json格式的结果
# 注：前三步交给get_df函数做了
def query(request):
    # six库主要是为了兼容python2和python3
    # 调用Python 2中的dictionary.iterlists() 或Python 3中的dictionary.lists()
    form_dict = dict(six.iterlists(request.GET))
    print("前端表单转换为字典：")
    print(form_dict)
    pivoted = get_df(form_dict)
    df=get_df(form_dict,is_pivoted=False)
    # KPI
    # kpi = get_kpi(pivoted)

    # table = ptable(pivoted)
    # 透视表格
    table = pivoted.to_html(#formatters=build_formatters_by_col(pivoted),  # 逐列调整表格内数字格式
        classes='ui selectable celled table',  # 指定表格css class为Semantic UI主题
        table_id='ptable'  # 指定表格id
    )
    # 原数据表格
    inittable = df.to_html(#formatters=build_formatters_by_col(pivoted),  # 逐列调整表格内数字格式
        classes='ui selectable celled table',  # 指定表格css class为Semantic UI主题
        table_id='initdata_table'  # 指定表格id
    )


    #describe和valuecounts函数转为图表
    info_chart=json.loads(prepare_chart(df, 'get_info_chart', index,column,aggfunc,value))
    # 原数据图
    origin_data_chart=json.loads(prepare_chart(df,'creat_origindata_chart',index,column,aggfunc,value))
    # 3d透视图
    pivot_chart = json.loads(prepare_chart(pivoted, 'get_pivot_chart',index,column,aggfunc,value))
    context = {
        'ptable':table,
        "initdata_table":inittable,
        'info_chart':info_chart,
        'pivot_chart': pivot_chart,
        'origin_data_chart':origin_data_chart
    }

    return HttpResponse(json.dumps(context, ensure_ascii=False), content_type="application/json charset=utf-8") # 返回结果必须是json格式

# 下面是一个获得各个字段option_list的简单方法,在页面初始化时从后端提取所有字段的不重复值作为选项传入前端。
def get_distinct_list(column, db_table):
    sql = "Select DISTINCT " + column + " From " + db_table
    df = pd.read_sql_query(sql, ENGINE)
    l = df.values.flatten().tolist()
    return l
# 构造sql语句
def sqlparse(context):
    sql = "Select * from %s Where true " % (DB_TABLE)  # 构造sql语句前半段

    # 下面循环处理多选部分（即数据筛选部分）
    for k, v in context.items():
        if k not in ['csrfmiddlewaretoken', 'DIMENSION_select', 'VALUE_select', 'INDEX_select','AGGFUNC_select']:
            if k[-2:] == '[]':
                field_name = k[:-9]  # 如果键以[]结尾，删除_select[]取原字段名
            else:
                field_name = k[:-7]  # 如果键不以[]结尾，删除_select取原字段名
            selected = v  # 选择项
            sql = sql_extent(sql, field_name, selected)  #未来可以通过进一步拼接字符串动态扩展sql语句
    return sql

# 通过AND关键字连接来扩展sql语句
def sql_extent(sql, field_name, selected, operator=" AND "):
    if selected is not None:
        statement = ''
        for data in selected:
            statement = statement + "'" + data + "', "
        statement = statement[:-2]
        if statement != '':
            sql = sql + operator + field_name + " in (" + statement + ")"
    return sql

# 可视化数据，渲染图表
def prepare_chart(df,  # 输入经过pivoted方法透视过的df，不是原始df
                  chart_type,  # 图表类型字符串，人为设置，根据图表类型不同做不同的Pandas数据处理，及生成不同的Pyechart对象
                  index,  # 前端表单字典，用来获得一些变量作为图表的标签如单位
                  column,
                  agg,
                  value):
    if chart_type=='get_info_chart':#渲染df.describe的出来的表格
        chart=creat_info_chart(df,index,column)
        return chart.dump_options()  # 用json格式返回Pyecharts图表对象的全局设置
    elif chart_type=='get_pivot_chart':
        chart=creat_pivot_chart(df,index,column,agg,value)
        return chart.dump_options()
    elif chart_type=='creat_origindata_chart':
        chart=creat_origindata_chart(df)
        return chart.dump_options()
    else:
        return None
@login_required
# 导出数据函数
def export(request, type):
    form_dict = dict(six.iterlists(request.GET))
    if type == 'pivoted':
        df = get_df(form_dict)  # 透视后的数据
        sheet_name='透视数据'
    elif type == 'raw':
        df = get_df(form_dict, is_pivoted=False)  # 原始数
        sheet_name='原始数据'
    excel_file = IO()

    xlwriter = pd.ExcelWriter(excel_file)

    df.to_excel(xlwriter, sheet_name=sheet_name, index=True)

    xlwriter.save()
    xlwriter.close()

    excel_file.seek(0)

    # 设置浏览器mime类型
    response = HttpResponse(excel_file.read(),
                            content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')

    # 设置文件名
    now = datetime.datetime.now().strftime("%Y%m%d%H%M%S")  # 当前精确时间不会重复，适合用来命名默认导出文件
    response['Content-Disposition'] = 'attachment; filename=' + now + '.xlsx'
    return response

# 读取用户导入的csv文件并将其存入mysql数据库(数据清洗也在这里)
class CsvToMysql(object):
    def __init__(self, hostname, port, user, passwd, db):
        self.dbname = db
        self.conn = connect(host=hostname, port=port, user=user, passwd=passwd, db=db)
        self.cursor = self.conn.cursor()

    # 读取csv文件
    def read_csv(self,filename):
        # 判断是否为空文件
        size = os.path.getsize(filename)
        if size == 0:
            return 'null'
        # csv文件中的字段可能会有空，在读取的时候会变成nan，nan到了mysql中是没有办法处理的就会报错，
        # 所以需要加上这个keep_default_na=False，设为false后就会保留原空字符，就不会变成nan了
        df = pd.read_csv(filename, keep_default_na=False, encoding='utf-8')
        table_name = '`'+os.path.split(filename)[-1].split('.')[0] + '`'
        print("下列语句测试构造出来的表名是否正确")
        print(os.path.split(filename))
        print(os.path.split(filename)[-1])
        print(os.path.split(filename)[-1].split('.'))
        print(os.path.split(filename)[-1].split('.')[0])
        self.csv2mysql(db_name=self.dbname,table_name=table_name, df=df )
        buffer = io.StringIO()
        df.info(buf=buffer,memory_usage='deep')
        s =buffer.getvalue()#获取到数据的元数据
        ss="您导入的数据的字段信息如下：\n"+s[s.rfind('Range'):]
        return ss
    # pandas的数据类型和MySQL是不通用的，需要进行类型转换。字段名可能含有非法字符，需要反引号。
    def make_table_sql(self,df):
        #将csv中的字段类型转换成mysql中的字段类型
        columns = df.columns.tolist()
        make_table = []
        make_field = []
        for col in columns:
            item1 = '`'+col+'`'
            if 'int' in str(df[col].dtype):
                char = item1 + ' FLOAT'
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
        print("开始构造表格：")
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


# 在python中，Unicode类型是作为编码的基础类型
#       decode                 encode
# str ---------> str(Unicode) ---------> str
# 得到文件的编码方式，方便读取文件时选择对应的编码方式
def get_file_code(content_type):
    try:
        content_type.decode('utf-8').encode('utf-8')
        source_encoding='utf-8'
    except:
        try:
            content_type.decode('gbk').encode('utf-8')
            source_encoding='gbk'
        except:
            try:
                content_type.decode('gb2312').encode('utf-8')
                source_encoding='gb2312'
            except:
                try:
                    content_type.decode('gb18030').encode('utf-8')
                    source_encoding='gb18030'
                except:
                    try:
                        content_type.decode('big5').encode('utf-8')
                        source_encoding='gb18030'
                    except:
                        content_type.decode('cp936').encode('utf-8')
                        source_encoding='cp936'
    return source_encoding

# 初始化前端表单

def init_html_form(df):
    D_screen_condition=dict(zip(df['COLUMN_NAME'],'`'+df['COLUMN_NAME']+'`'))
    #传给前端value选择的备选值,因为value只能选择数值类型的数据,选择字符数据没有意义
    df2=df[df['DATA_TYPE'].isin(['int','float'])]
    D_screen_condition2VALUE=dict(zip(df2['COLUMN_NAME'],'`'+df2['COLUMN_NAME']+'`'))
    print("index/column备选项为:")
    print(D_screen_condition)
    print("value备选项为:")
    print(D_screen_condition2VALUE)
    # 下面的代码负责初始化表单选项(index和column)
    mselect_dict = {}
    for key, value in D_screen_condition.items():
        #mselect_dict的key为D_screen_condition的key
        # mselect_dict的value为字典，具有select和options两个字段
        mselect_dict[key] = {}

        # value的select字段表示选择了数据库中的哪个属性
        mselect_dict[key]['select'] = value

        # value的options字段表示数据库中该属性具有的各不相同的取值
        option_list=get_distinct_list(value,DB_TABLE)
        mselect_dict[key]['options'] = option_list  #以后可以后端通过列表为每个多选控件传递备选项
        # 下面单独初始化value备选框
    mselect_dict_value={}
    for key, value in D_screen_condition2VALUE.items():
        #D_MULTI_SELECT
        #mselect_dict_value的key为D_screen_condition2VALUE的key
        # mselect_dict_value的value为字典，具有select和options两个字段
        mselect_dict_value[key] = {}

        # value的select字段表示选择了数据库中的哪个属性
        mselect_dict_value[key]['select'] = value

        # value的options字段表示数据库中该属性具有的各不相同的取值
        option_list=get_distinct_list(value,DB_TABLE)
        mselect_dict_value[key]['options'] = option_list  #以后可以后端通过列表为每个多选控件传递备选项
    return mselect_dict,mselect_dict_value