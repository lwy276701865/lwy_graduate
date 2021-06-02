from pyecharts.charts import Line, Bar, Grid,Bar3D
from pyecharts import options as opts
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.font_manager as fm
import matplotlib as mpl
from matplotlib.ticker import FuncFormatter
from adjustText import adjust_text
from io import BytesIO
import base64
import scipy.stats as stats
import seaborn as sns
import matplotlib.style as mplstyle
from eplot import eplot
import pandas as pd
myfont = fm.FontProperties(fname='C:/Windows/Fonts/msyh.ttc')

# 此函数创建三个基本信息图表
def creat_info_chart(df,index,column):
    bar=(
        Bar()
            .add_xaxis(list(df[index].value_counts().index))
            .add_yaxis('COUNT('+index+')',df[index].value_counts().values.tolist())
            .set_global_opts(
            datazoom_opts=[opts.DataZoomOpts(xaxis_index=[0,1],pos_left='center',pos_top='middle'), opts.DataZoomOpts(type_="inside",xaxis_index=[0,1],pos_left='center',pos_top='middle')],
            toolbox_opts=opts.ToolboxOpts(orient='vertical',pos_left='90%'),
            tooltip_opts=opts.TooltipOpts(trigger="axis", axis_pointer_type="cross"),
            title_opts=opts.TitleOpts(title=column+"分布"),
            legend_opts=opts.LegendOpts(pos_left="65%"),
            yaxis_opts=opts.AxisOpts(name="个数"),
            xaxis_opts=opts.AxisOpts(name=index),
        )
    )
    bar2=(
        Bar()
            .add_xaxis(list(df[column].value_counts().index))
            .add_yaxis('COUNT('+column+')',df[column].value_counts().values.tolist())
            .set_global_opts(
            # datazoom_opts=[opts.DataZoomOpts(xaxis_index=[1,2]), opts.DataZoomOpts(type_="inside",xaxis_index=[1,2])],
            title_opts=opts.TitleOpts(title=index+"分布",pos_left="50%"),
            toolbox_opts=opts.ToolboxOpts(orient='vertical',pos_left='90%'),
            tooltip_opts=opts.TooltipOpts(trigger="axis", axis_pointer_type="cross"),
            legend_opts=opts.LegendOpts(pos_left="25%"),
            yaxis_opts=opts.AxisOpts(name="个数"),
            xaxis_opts=opts.AxisOpts(name=column),
        )
    )
    # count:统计个数；unique：表示有多少种不同的值；top：数据中出现次数最高的值；freq：出现次数最高的那个值（top）的出现频率

    # 百分位数：第p百分位数是这样一个值，它使得至少有p%的数据项小于或等于这个值，且至少有(100-p)%的数据项大于或等于
    # 这个值。以身高为例，身高分布的第五百分位表示有5%的人的身高小于此测量值，95%的身高大于此测量值。

    x_data = list(df.describe(include='all').index)
    yaxis=list(df.describe(include='all').columns)
    line=Line()
    line.add_xaxis(xaxis_data=x_data)
    for i in range(len(yaxis)):
        line.add_yaxis(
            series_name=yaxis[i],
            stack='info_data',
            y_axis=df.describe(include='all')[yaxis[i]],
            label_opts=opts.LabelOpts(is_show=False),
        )
    line.set_global_opts(
        toolbox_opts=opts.ToolboxOpts(orient='vertical',pos_left='90%'),
        title_opts=opts.TitleOpts(title="原数据统计信息",pos_top="50%"),
        tooltip_opts=opts.TooltipOpts(trigger="axis", axis_pointer_type="cross"),
        legend_opts=opts.LegendOpts(pos_bottom='1%',type_='scroll'),
        yaxis_opts=opts.AxisOpts(
            type_="category",
            axistick_opts=opts.AxisTickOpts(is_show=True),
            splitline_opts=opts.SplitLineOpts(is_show=True),
        ),
        xaxis_opts=opts.AxisOpts(type_="category", boundary_gap=False),
    )
    grid = (
        Grid()
            .add(bar, grid_opts=opts.GridOpts(pos_bottom="60%",pos_left="55%"))
            .add(bar2, grid_opts=opts.GridOpts(pos_bottom="60%",pos_right="55%"))
            .add(line, grid_opts=opts.GridOpts(pos_top="60%"))

    )
    return grid
# 创建原数据图
def creat_origindata_chart(df):
    eplot.set_config(return_type='CHART')
    chart=df.eplot.bar().set_global_opts(
        datazoom_opts=[opts.DataZoomOpts(), opts.DataZoomOpts(type_="inside")],
        tooltip_opts=opts.TooltipOpts(trigger="axis", axis_pointer_type="cross",position='bottom'),
        toolbox_opts=opts.ToolboxOpts(orient='vertical',pos_left='90%',pos_top='10%'),
        legend_opts=opts.LegendOpts(type_='scroll'),
        xaxis_opts=opts.AxisOpts(name='原数据索引值')
    )
    return chart
# 创建透视表的图
def creat_pivot_chart(pivot,index,column,agg,value):
    index_list=pivot.index.tolist()
    column_list=pivot.columns.tolist()
    if pivot.max().tolist():
        visualmap_max=max(pivot.max().tolist())
        range_text=['最大值', '最小值']
    else:
        visualmap_max=1
        range_text=['无', '数据']
    # print(pivot.max())
    data = [(i, j, int(pivot.iloc[i,j])) for i in range(len(index_list)) for j in range(len(column_list))]
    c = (
        Bar3D()
            .add(
            series_name=agg+'('+value+')',
            data=data,
            shading="lambert",
            xaxis3d_opts=opts.Axis3DOpts(index_list, type_="category",name=index),
            yaxis3d_opts=opts.Axis3DOpts(column_list, type_="category",name=column),
            zaxis3d_opts=opts.Axis3DOpts(type_="value",name=agg+'('+value+')'),
        )
            .set_global_opts(
            visualmap_opts=opts.VisualMapOpts(max_=visualmap_max,range_text=range_text),
            title_opts=opts.TitleOpts(title="数据透视三维图"),
            toolbox_opts=opts.ToolboxOpts(),
            datazoom_opts=opts.DataZoomOpts(type_="inside")
        )
    )
    return c
