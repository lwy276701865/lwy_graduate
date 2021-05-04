import json
from django.http import HttpResponseRedirect
import os
from django.shortcuts import render
# Create your views here.
from django.http import HttpResponse
from pymysql import connect
from sqlalchemy import create_engine
import sqlalchemy as sa
import pandas as pd
import numpy as np
import six
from .charts import *
try:
    from io import BytesIO as IO # for modern python
except ImportError:
    from io import StringIO as IO # for legacy python
import datetime
import xlsxwriter
from django.views.decorators.cache import cache_page
from django.contrib.auth.decorators import login_required
ENGINE = create_engine('mysql+pymysql://root:lwydecd+20@localhost:3306/test') #创建数据库连接引擎
# 根据用户传进来的数据创建的表
DB_TABLE='example'
@login_required
# 此函数初始化界面，然后用户点击导入按钮后自动生成可选条件，让用户选择
def index(request):
    mselect_dict = {}
    for key, value in D_MULTI_SELECT.items():
        #mselect_dict的key为D_MULTI_SELECT的key
        # mselect_dict的value为字典，具有select和options两个字段
        mselect_dict[key] = {}
        # value的select字段表示选择了数据库中的哪个属性
        mselect_dict[key]['select'] = value
        # value的options字段表示数据库中该属性具有的各不相同的取值
        option_list=get_distinct_list(value,DB_TABLE)
        mselect_dict[key]['options'] = option_list  #以后可以后端通过列表为每个多选控件传递备选项
    context = {
        'mselect_dict': mselect_dict
    }
    return render(request, 'chpa_data/display.html', context)
# 此函数根据form_dict数据做对应的处理（处理为原数据或者透视数据表）
def get_df(form_dict, is_pivoted=True):
    sql = sqlparse(form_dict)  # sql拼接
    df = pd.read_sql_query(sa.text(sql), ENGINE)  # 将sql语句结果读取至Pandas Dataframe

    if is_pivoted is True:
        dimension_selected = form_dict['DIMENSION_select'][0]
        if dimension_selected[0] == '`':
            column = dimension_selected[1:][:-1]
        else:
            column = dimension_selected

        pivoted = pd.pivot_table(df,
                                 values='AMOUNT',  # 数据透视汇总值为AMOUNT字段，一般保持不变
                                 index='DATE',  # 数据透视行为DATE字段，一般保持不变
                                 columns=column,  # 数据透视列为前端选择的分析维度
                                 aggfunc=np.sum)  # 数据透视汇总方式为求和，一般保持不变
        if pivoted.empty is False:
            pivoted.sort_values(by=pivoted.index[-1], axis=1, ascending=False, inplace=True)  # 结果按照最后一个DATE表现排序

        return pivoted
    else:
        return df
@login_required
@cache_page(60 * 60 * 24 * 30) #  缓存30天
# 此函数在前端选择了筛选条件之后通过前端传递过来的值进行分析，并返回json格式的结果
# 1.解析前端参数到理想格式
# 2.根据前端参数数据拼接SQL并用Pandas
# 3.读取Pandas读取数据后，将前端选择的DIMENSION作为pivot_table方法的column参数
# 4.返回Json格式的结果
def query(request):
    form_dict = dict(six.iterlists(request.GET))
    pivoted = get_df(form_dict)

    # KPI
    kpi = get_kpi(pivoted)

    table = ptable(pivoted)
    table = table.to_html(formatters=build_formatters_by_col(table),  # 逐列调整表格内数字格式
                          classes='ui selectable celled table',  # 指定表格css class为Semantic UI主题
                          table_id='ptable'  # 指定表格id
                          )

    # Pyecharts交互图表
    bar_total_trend = json.loads(prepare_chart(pivoted, 'bar_total_trend', form_dict))

    # Matplotlib静态图表
    bubble_performance = prepare_chart(pivoted, 'bubble_performance', form_dict)
    context = {
        "market_size": kpi["market_size"],
        "market_gr": kpi["market_gr"],
        "market_cagr": kpi["market_cagr"],
        'ptable': table,
        'bar_total_trend': bar_total_trend,
        'bubble_performance': bubble_performance
    }

    return HttpResponse(json.dumps(context, ensure_ascii=False), content_type="application/json charset=utf-8") # 返回结果必须是json格式
def get_kpi(df):

    # 按列求和为市场总值的Series
    market_total = df.sum(axis=1)#求每一行的和
    # iloc[-1]为数据最后一行（最后一个DATE）就是最新的市场规模
    market_size = market_total.iloc[-1]
    # 市场按列求和，倒数第5行（倒数第5个DATE）就是同比的市场规模，可以用来求同比增长率
    #同比增长率=（当年的指标值-去年同期的值）÷去年同期的值*100%
    market_gr = market_total.iloc[-1] / market_total.iloc[-5] - 1

    #复合增长率的英文缩写为：CAGR（Compound Annual Growth Rate）

    # 复合增长率是一项投资在特定时期内的年度增长率 计算方法为总增长率百分比的n方根，n相等于有关时期内的年数
    # 公式为： （现有价值/基础价值）^(1/年数) - 1

    # 举个例子，你在2005年1月1日最初投资了1万元，而到了2006年1月1日你的资产增长到了1.3万元，到了2007年增长到了1.4万元，
    # 而到了2008年1月1日变为1.95万元。
    # 根据计算公式，你这笔投资的年复合增长率为：
    # 最终资金除以起始资金 ：19,500/10,000 = 1.95
    # 开立方（乘以1/年数的次方，这里即乘以1/3次方）1.95^(1/3) =1.2493
    # 将结果减去1.2493 -1=0.2493

    # 因为数据第一年是四年前的同期季度，时间序列收尾相除后开四次方根可得到年复合增长率

    market_cagr = (market_total.iloc[-1] / market_total.iloc[0]) ** (0.25) - 1
    if market_size == np.inf or market_size == -np.inf:
        market_size = "N/A"
    if market_gr == np.inf or market_gr == -np.inf:
        market_gr = "N/A"
    if market_cagr == np.inf or market_cagr == -np.inf:
        market_cagr = "N/A"
    #此时kpi是字典类型
    return {
        "market_size": market_size,
        "market_gr": market_gr,
        "market_cagr": market_cagr,
    }
    # list1=['market_size','market_gr','market_cagr']
    # list2=[market_size, "{0:.1%}".format(market_gr), "{0:.1%}".format(market_cagr)]
    # return dict(zip(list1,list2))
def ptable(df):
    # 份额
    # transform：data里每个元素位置的取值由transform函数的参数函数计算
    #每一个位置的元素除以这一行的所有元素的和
    df_share = df.transform(lambda x: x/x.sum(), axis=1)

    # 同比增长率，要考虑分子为0的问题
    #pct_change：表示当前元素与先前元素的相差百分比，当然指定periods=n,表示当前元素与先前n 个元素的相差百分比。
    df_gr = df.pct_change(periods=4)
    df_gr.dropna(how='all',inplace=True)
    df_gr.replace([np.inf, -np.inf], np.nan, inplace=True)

    # 最新滚动年绝对值表现及同比净增长
    df_latest = df.iloc[-1,:]
    df_latest_diff = df.iloc[-1,:] - df.iloc[-5,:]

    # 最新滚动年份额表现及同比份额净增长
    df_share_latest = df_share.iloc[-1, :]
    df_share_latest_diff = df_share.iloc[-1, :] - df_share.iloc[-5, :]

    # 进阶指标EI，衡量与市场增速的对比，高于100则为跑赢大盘
    df_gr_latest = df_gr.iloc[-1,:]
    df_total_gr_latest = df.sum(axis=1).iloc[-1]/df.sum(axis=1).iloc[-5] -1
    df_ei_latest = (df_gr_latest+1)/(df_total_gr_latest+1)*100

    df_combined = pd.concat([df_latest, df_latest_diff, df_share_latest, df_share_latest_diff, df_gr_latest, df_ei_latest], axis=1)
    df_combined.columns = ['最新滚动年销售额',
                           '净增长',
                           '份额',
                           '份额同比变化',
                           '同比增长率',
                           'EI']

    return df_combined
# 下面是一个获得各个字段option_list的简单方法,在页面初始化时从后端提取所有字段的不重复值作为选项传入前端。
def get_distinct_list(column, db_table):
    sql = "Select DISTINCT " + column + " From " + db_table
    df = pd.read_sql_query(sql, ENGINE)
    l = df.values.flatten().tolist()
    return l
# 构造sql语句
def sqlparse(context):
    print(context)
    sql = "Select * from %s Where PERIOD = '%s' And UNIT = '%s'" % \
          (DB_TABLE, context['PERIOD_select'][0], context['UNIT_select'][0])  # 先处理单选部分

    # 下面循环处理多选部分
    for k, v in context.items():
        if k not in ['csrfmiddlewaretoken', 'DIMENSION_select', 'PERIOD_select', 'UNIT_select']:
            if k[-2:] == '[]':
                field_name = k[:-9]  # 如果键以[]结尾，删除_select[]取原字段名
            else:
                field_name = k[:-7]  # 如果键不以[]结尾，删除_select取原字段名
            selected = v  # 选择项
            sql = sql_extent(sql, field_name, selected)  #未来可以通过进一步拼接字符串动态扩展sql语句
    return sql


def sql_extent(sql, field_name, selected, operator=" AND "):
    if selected is not None:
        statement = ''
        for data in selected:
            statement = statement + "'" + data + "', "
        statement = statement[:-2]
        if statement != '':
            sql = sql + operator + field_name + " in (" + statement + ")"
    return sql
# 该字典key为前端准备显示的所有多选字段名, value为数据库对应的字段名
D_MULTI_SELECT = {
    'TC I': '`TC I`',
    'TC II': '`TC II`',
    'TC III': '`TC III`',
    'TC IV': '`TC IV`',
    '通用名|MOLECULE': 'MOLECULE',
    '商品名|PRODUCT': 'PRODUCT',
    '包装|PACKAGE': 'PACKAGE',
    '生产企业|CORPORATION': 'CORPORATION',
    '企业类型': 'MANUF_TYPE',
    '剂型': 'FORMULATION',
    '剂量': 'STRENGTH'
}
def build_formatters_by_col(df):
    format_abs = lambda x: '{:,.0f}'.format(x)
    format_share = lambda x: '{:.1%}'.format(x)
    format_gr = lambda x: '{:.1%}'.format(x)
    format_currency = lambda x: '¥{:,.0f}'.format(x)
    d = {}
    for column in df.columns:
        if '份额' in column or '贡献' in column:
            d[column] = format_share
        elif '价格' in column or '单价' in column:
            d[column] = format_currency
        elif '同比增长' in column or '增长率' in column or 'CAGR' in column or '同比变化' in column:
            d[column] = format_gr
        else:
            d[column] = format_abs
    return d
D_TRANS = {
    'MAT': '滚动年',
    'QTR': '季度',
    'Value': '金额',
    'Volume': '盒数',
    'Volume (Counting Unit)': '最小制剂单位数',
    '滚动年': 'MAT',
    '季度': 'QTR',
    '金额': 'Value',
    '盒数': 'Volume',
    '最小制剂单位数': 'Volume (Counting Unit)'
}

def prepare_chart(df,  # 输入经过pivoted方法透视过的df，不是原始df
                  chart_type,  # 图表类型字符串，人为设置，根据图表类型不同做不同的Pandas数据处理，及生成不同的Pyechart对象
                  form_dict,  # 前端表单字典，用来获得一些变量作为图表的标签如单位
                  ):
    label = D_TRANS[form_dict['PERIOD_select'][0]] + D_TRANS[form_dict['UNIT_select'][0]]

    if chart_type == 'bar_total_trend':
        df_abs = df.sum(axis=1)  # Pandas列汇总，返回一个N行1列的series，每行是一个date的市场综合
        # df_abs.index = df_abs.index.strftime("%Y-%m")
        # df_abs.index = datetime.strptime(str(df_abs.index),'%Y-%m-%d %H:%M:%S.%f').strftime("%Y-%m")
        df_abs.index =pd.to_datetime(df_abs.index,format="%Y-%m")  # 行索引日期数据变成2020-06的形式
        df_abs = df_abs.to_frame()  # series转换成df
        df_abs.columns = [label]  # 用一些设置变量为系列命名，准备作为图表标签
        df_gr = df_abs.pct_change(periods=4)  # 获取同比增长率
        df_gr.dropna(how='all', inplace=True)  # 删除没有同比增长率的行，也就是时间序列数据的最前面几行，他们没有同比
        df_gr.replace([np.inf, -np.inf, np.nan], '-', inplace=True)  # 所有分母为0或其他情况导致的inf和nan都转换为'-'
        chart = echarts_stackbar(df=df_abs,
                                 df_gr=df_gr
                                 )  # 调用stackbar方法生成Pyecharts图表对象
        return chart.dump_options()  # 用json格式返回Pyecharts图表对象的全局设置
    elif chart_type == 'bubble_performance':
        df_abs = df.iloc[-1,:]  # 获取最新时间粒度的绝对值
        df_share = df.transform(lambda x: x / x.sum(), axis=1).iloc[-1,:] # 获取份额
        df_diff = df.diff(periods=4).iloc[-1,:]  # 获取同比净增长

        chart = mpl_bubble(x=df_abs,  # x轴数据
                           y=df_diff,  # y轴数据
                           z=df_share * 50000,  # 气泡大小数据
                           labels=df.columns.str.split('|').str[0],  # 标签数据
                           title='',  # 图表标题
                           x_title=label,  # x轴标题
                           y_title=label + '净增长',  # y轴标题
                           x_fmt='{:,.0f}',  # x轴格式
                           y_fmt='{:,.0f}',  # y轴格式
                           y_avg_line=True,  # 添加y轴分隔线
                           y_avg_value=0,  # y轴分隔线为y=0
                           label_limit=30  # 只显示前30个项目的标签
                           )
        return chart
    else:
        return None
@login_required
@cache_page(60 * 60 * 24 * 30)
# 导出数据函数
def export(request, type):
    form_dict = dict(six.iterlists(request.GET))

    if type == 'pivoted':
        df = get_df(form_dict)  # 透视后的数据
    elif type == 'raw':
        df = get_df(form_dict, is_pivoted=False)  # 原始数

    excel_file = IO()

    xlwriter = pd.ExcelWriter(excel_file)

    df.to_excel(xlwriter, 'data', index=True)

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
