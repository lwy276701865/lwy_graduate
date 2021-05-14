from pyecharts.charts import Line, Bar, Grid
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
            title_opts=opts.TitleOpts(title="column分布情况"),
            legend_opts=opts.LegendOpts(pos_left="65%"),
            yaxis_opts=opts.AxisOpts(name="CountNum"),
            xaxis_opts=opts.AxisOpts(name=index),
        )
    )
    bar2=(
        Bar()
            .add_xaxis(list(df[column].value_counts().index))
            .add_yaxis('COUNT('+column+')',df[column].value_counts().values.tolist())
            .set_global_opts(
            # datazoom_opts=[opts.DataZoomOpts(xaxis_index=[1,2]), opts.DataZoomOpts(type_="inside",xaxis_index=[1,2])],
            title_opts=opts.TitleOpts(title="index分布情况",pos_left="50%"),
            toolbox_opts=opts.ToolboxOpts(orient='vertical',pos_left='90%'),
            tooltip_opts=opts.TooltipOpts(trigger="axis", axis_pointer_type="cross"),
            legend_opts=opts.LegendOpts(pos_left="25%"),
            yaxis_opts=opts.AxisOpts(name="CountNum"),
            xaxis_opts=opts.AxisOpts(name=column),
        )
    )
    x_data = list(df.describe().index)
    yaxis=list(df.describe().columns)
    line=Line()
    line.add_xaxis(xaxis_data=x_data)
    for i in range(len(yaxis)):
        line.add_yaxis(
            series_name=yaxis[i],
            stack='info_data',
            y_axis=df.describe()[yaxis[i]],
            label_opts=opts.LabelOpts(is_show=False),
        )
    line.set_global_opts(
        toolbox_opts=opts.ToolboxOpts(orient='vertical',pos_left='90%'),
        title_opts=opts.TitleOpts(title="原数据基本信息图",pos_top="50%"),
        tooltip_opts=opts.TooltipOpts(trigger="axis", axis_pointer_type="cross"),
        legend_opts=opts.LegendOpts(pos_top="55%"),
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
def creat_pivot_chart(df):
    eplot.set_config(return_type='CHART')
    chart=df.eplot.bar().set_global_opts(
        # title_opts=opts.TitleOpts(title="Bar-DataZoom（slider+inside）"),
        datazoom_opts=[opts.DataZoomOpts(), opts.DataZoomOpts(type_="inside")],
        yaxis_opts=opts.AxisOpts(name='我是y轴'),
        xaxis_opts=opts.AxisOpts(name='我是x轴'),
        tooltip_opts=opts.TooltipOpts(trigger="axis", axis_pointer_type="cross",position='bottom'),
        toolbox_opts=opts.ToolboxOpts(orient='vertical',pos_left='90%',pos_top='10%'),
        legend_opts=opts.LegendOpts(type_='scroll')
    )
    return chart
