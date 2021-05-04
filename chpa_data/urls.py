from django.urls import path,re_path
from . import views

app_name = 'chpa'  # 这句是必须的，和之后所有的URL语句有关
urlpatterns = [
    #r’index/’中的r 表示这是一个原始字符串，这样避免了使用过多的转义符
    #第一个index对应实际输入的地址里面的index
    #第二个index对应views.py里的index方法
    #第三个index对应Django特色的url tag，即header.html里的
    # <a href={% url "chpa:index" %} class="item">首页</a>
    path(r'index', views.index, name='index'),
    path(r'search/<str:column>/<str:kw>', views.search, name='search'),
    path(r'query', views.query, name='query'),
    path(r'export/<str:type>', views.export, name='export'),
]