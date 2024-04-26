import os
import pandas as pd
from pyecharts.charts import Bar, HeatMap, Grid,Page,Pie
import pyecharts.options as opts
import re
from pyecharts.globals import ThemeType
from pyecharts.charts import WordCloud

def add_line(sentence,interval=6):
    sentence = sentence.replace(" ","")
    string = "(.{" + str(interval) + "})"
    sentence = re.sub(string,"\\1\n",sentence)
    return sentence

def add_line_eng(sentence):
    sentence = sentence.replace(" ","\n")
    return sentence

def show_view_data(video_data_csv,up_data_csv,base_dir):
    # 读取视频的数据
    video_data = pd.read_csv(video_data_csv)
    # 对长标题进行换行
    video_data['title'] = [add_line(i) for i in video_data['title']]
    # 读取up主的数据
    up_data = pd.read_csv(up_data_csv)
    # 对长的名字进行换行
    up_data['up'] = [add_line(i) for i in up_data['up']]
    # 绘制柱状图
    video_bar = (Bar(init_opts=opts.InitOpts(theme=ThemeType.ESSOS,width="1200px"),)
                  .add_xaxis(video_data['title'].tolist())
                  .add_yaxis('观看次数',video_data['view_data'].tolist())
                  .set_global_opts(title_opts=opts.TitleOpts(title='播放量最多的视频',pos_left='center',pos_top='5%'),
                                    toolbox_opts=opts.TitleOpts(),
                                    legend_opts=opts.LegendOpts(is_show=False),
                                    xaxis_opts=opts.AxisOpts(axislabel_opts=opts.LabelOpts(rotate=0,font_size = 10),))
                 .set_series_opts(label_opts=opts.LabelOpts(is_show=False))
    )
    # 绘制柱状图
    up_bar = (Bar(init_opts=opts.InitOpts(theme=ThemeType.ESSOS,width="1200px"))
                  .add_xaxis(up_data['up'].tolist())
                  .add_yaxis('观看次数',up_data['view_data'].tolist())
                  .set_global_opts(title_opts=opts.TitleOpts(title='播放量累计的up主',pos_left='center',pos_top='5%'),
                                    toolbox_opts=opts.TitleOpts(),
                                    legend_opts=opts.LegendOpts(is_show=False),
                                    xaxis_opts=opts.AxisOpts(axislabel_opts=opts.LabelOpts(rotate=0,font_size = 10),))
                  .set_series_opts(label_opts=opts.LabelOpts(is_show=False))
    )
    # 通过Page将两个柱状图按上下分布保存在同一个html中
    save_dir = os.path.join(base_dir,'view_data.html')
    page = Page(layout=Page.SimplePageLayout)
    page.add(video_bar,up_bar)
    page.render(save_dir)


def show_coin_data(video_data_csv,up_data_csv,base_dir):
    video_data = pd.read_csv(video_data_csv)
    video_data['title'] = [add_line(i) for i in video_data['title']]
    up_data = pd.read_csv(up_data_csv)
    up_data['up'] = [add_line(i) for i in up_data['up']]
    video_bar = (Bar(init_opts=opts.InitOpts(theme=ThemeType.ESSOS,width="1200px"),)
                  .add_xaxis(video_data['title'].tolist())
                  .add_yaxis('投币数',video_data['coin_data'].tolist())
                  .set_global_opts(title_opts=opts.TitleOpts(title='投币最多的视频',pos_left='center',pos_top='5%'),
                                    toolbox_opts=opts.TitleOpts(),
                                    legend_opts=opts.LegendOpts(is_show=False),
                                    xaxis_opts=opts.AxisOpts(axislabel_opts=opts.LabelOpts(rotate=0,font_size = 10),))
                 .set_series_opts(label_opts=opts.LabelOpts(is_show=False))
    )
    up_bar = (Bar(init_opts=opts.InitOpts(theme=ThemeType.ESSOS,width="1200px"))
                  .add_xaxis(up_data['up'].tolist())
                  .add_yaxis('投币数',up_data['coin_data'].tolist())
                  .set_global_opts(title_opts=opts.TitleOpts(title='投币最多的up主',pos_left='center',pos_top='5%'),
                                    toolbox_opts=opts.TitleOpts(),
                                    legend_opts=opts.LegendOpts(is_show=False),
                                    xaxis_opts=opts.AxisOpts(axislabel_opts=opts.LabelOpts(rotate=0,font_size = 10),))
                  .set_series_opts(label_opts=opts.LabelOpts(is_show=False))
    )
    save_dir = os.path.join(base_dir,'coin_data.html')
    page = Page(layout=Page.SimplePageLayout)
    page.add(video_bar,up_bar)
    page.render(save_dir)

def show_danmaku_data(video_data_csv,up_data_csv,base_dir):
    video_data = pd.read_csv(video_data_csv)
    video_data['title'] = [add_line(i) for i in video_data['title']]
    up_data = pd.read_csv(up_data_csv)
    up_data['up'] = [add_line(i) for i in up_data['up']]
    video_bar = (Bar(init_opts=opts.InitOpts(theme=ThemeType.ESSOS,width="1200px"),)
                  .add_xaxis(video_data['title'].tolist())
                  .add_yaxis('弹幕数',video_data['danmaku_data'].tolist())
                  .set_global_opts(title_opts=opts.TitleOpts(title='弹幕数最多的视频',pos_left='center',pos_top='5%'),
                                    toolbox_opts=opts.TitleOpts(),
                                    legend_opts=opts.LegendOpts(is_show=False),
                                    xaxis_opts=opts.AxisOpts(axislabel_opts=opts.LabelOpts(rotate=0,font_size = 9),))
                 .set_series_opts(label_opts=opts.LabelOpts(is_show=False))
    )
    up_bar = (Bar(init_opts=opts.InitOpts(theme=ThemeType.ESSOS,width="1200px"))
                  .add_xaxis(up_data['up'].tolist())
                  .add_yaxis('弹幕数',up_data['danmaku_data'].tolist())
                  .set_global_opts(title_opts=opts.TitleOpts(title='弹幕总数最多的up主',pos_left='center',pos_top='5%'),
                                    toolbox_opts=opts.TitleOpts(),
                                    legend_opts=opts.LegendOpts(is_show=False),
                                    xaxis_opts=opts.AxisOpts(axislabel_opts=opts.LabelOpts(rotate=0,font_size = 10),))
                  .set_series_opts(label_opts=opts.LabelOpts(is_show=False))
    )
    save_dir = os.path.join(base_dir,'danmaku_data.html')
    page = Page(layout=Page.SimplePageLayout)
    page.add(video_bar,up_bar)
    page.render(save_dir)

def show_favorite_data(video_data_csv,up_data_csv,base_dir):
    video_data = pd.read_csv(video_data_csv)
    video_data['title'] = [add_line(i) for i in video_data['title']]
    up_data = pd.read_csv(up_data_csv)
    up_data['up'] = [add_line(i) for i in up_data['up']]
    video_bar = (Bar(init_opts=opts.InitOpts(theme=ThemeType.ESSOS,width="1200px"),)
                  .add_xaxis(video_data['title'].tolist())
                  .add_yaxis('收藏数',video_data['favorite_data'].tolist())
                  .set_global_opts(title_opts=opts.TitleOpts(title='收藏最多的视频',pos_left='center',pos_top='5%'),
                                    toolbox_opts=opts.TitleOpts(),
                                    legend_opts=opts.LegendOpts(is_show=False),
                                    xaxis_opts=opts.AxisOpts(axislabel_opts=opts.LabelOpts(rotate=0,font_size = 10),))
                 .set_series_opts(label_opts=opts.LabelOpts(is_show=False))
    )
    up_bar = (Bar(init_opts=opts.InitOpts(theme=ThemeType.ESSOS,width="1200px"))
                  .add_xaxis(up_data['up'].tolist())
                  .add_yaxis('收藏数',up_data['favorite_data'].tolist())
                  .set_global_opts(title_opts=opts.TitleOpts(title='收藏次数最多的up主',pos_left='center',pos_top='5%'),
                                    toolbox_opts=opts.TitleOpts(),
                                    legend_opts=opts.LegendOpts(is_show=False),
                                    xaxis_opts=opts.AxisOpts(axislabel_opts=opts.LabelOpts(rotate=0,font_size = 10),))
                  .set_series_opts(label_opts=opts.LabelOpts(is_show=False))
    )
    save_dir = os.path.join(base_dir,'favorite_data.html')
    page = Page(layout=Page.SimplePageLayout)
    page.add(video_bar,up_bar)
    page.render(save_dir)

def show_like_data(video_data_csv,up_data_csv,base_dir):
    video_data = pd.read_csv(video_data_csv)
    video_data['title'] = [add_line(i) for i in video_data['title']]
    up_data = pd.read_csv(up_data_csv)
    up_data['up'] = [add_line(i) for i in up_data['up']]
    video_bar = (Bar(init_opts=opts.InitOpts(theme=ThemeType.ESSOS,width="1200px"),)
                  .add_xaxis(video_data['title'].tolist())
                  .add_yaxis('点赞数',video_data['like_data'].tolist())
                  .set_global_opts(title_opts=opts.TitleOpts(title='点赞数最多的视频',pos_left='center',pos_top='5%'),
                                    toolbox_opts=opts.TitleOpts(),
                                    legend_opts=opts.LegendOpts(is_show=False),
                                    xaxis_opts=opts.AxisOpts(axislabel_opts=opts.LabelOpts(rotate=0,font_size = 10),))
                 .set_series_opts(label_opts=opts.LabelOpts(is_show=False))
    )
    up_bar = (Bar(init_opts=opts.InitOpts(theme=ThemeType.ESSOS,width="1200px"))
                  .add_xaxis(up_data['up'].tolist())
                  .add_yaxis('点赞数',up_data['like_data'].tolist())
                  .set_global_opts(title_opts=opts.TitleOpts(title='点赞数最多的up主',pos_left='center',pos_top='5%'),
                                    toolbox_opts=opts.TitleOpts(),
                                    legend_opts=opts.LegendOpts(is_show=False),
                                    xaxis_opts=opts.AxisOpts(axislabel_opts=opts.LabelOpts(rotate=0,font_size = 10),))
                 .set_series_opts(label_opts=opts.LabelOpts(is_show=False))
    )
    save_dir = os.path.join(base_dir,'like_data.html')
    page = Page(layout=Page.SimplePageLayout)
    page.add(video_bar,up_bar)
    page.render(save_dir)

def show_reply_data(video_data_csv,up_data_csv,base_dir):
    video_data = pd.read_csv(video_data_csv)
    video_data['title'] = [add_line(i) for i in video_data['title']]
    up_data = pd.read_csv(up_data_csv)
    up_data['up'] = [add_line(i) for i in up_data['up']]
    video_bar = (Bar(init_opts=opts.InitOpts(theme=ThemeType.ESSOS,width="1200px"),)
                  .add_xaxis(video_data['title'].tolist())
                  .add_yaxis('评论数',video_data['reply_data'].tolist())
                  .set_global_opts(title_opts=opts.TitleOpts(title='评论最多的视频',pos_left='center',pos_top='5%'),
                                    toolbox_opts=opts.TitleOpts(),
                                    legend_opts=opts.LegendOpts(is_show=False),
                                    xaxis_opts=opts.AxisOpts(axislabel_opts=opts.LabelOpts(rotate=0,font_size = 10),))
                 .set_series_opts(label_opts=opts.LabelOpts(is_show=False))
    )
    up_bar = (Bar(init_opts=opts.InitOpts(theme=ThemeType.ESSOS,width="1200px"))
                  .add_xaxis(up_data['up'].tolist())
                  .add_yaxis('评论数',up_data['reply_data'].tolist())
                  .set_global_opts(title_opts=opts.TitleOpts(title='评论最多的up主',pos_left='center',pos_top='5%'),
                                    toolbox_opts=opts.TitleOpts(),
                                    legend_opts=opts.LegendOpts(is_show=False),
                                    xaxis_opts=opts.AxisOpts(axislabel_opts=opts.LabelOpts(rotate=0,font_size = 10),))
              .set_series_opts(label_opts=opts.LabelOpts(is_show=False))
    )
    save_dir = os.path.join(base_dir,'reply_data.html')
    page = Page(layout=Page.SimplePageLayout)
    page.add(video_bar,up_bar)
    page.render(save_dir)

def show_share_data(video_data_csv,up_data_csv,base_dir):
    video_data = pd.read_csv(video_data_csv)
    video_data['title'] = [add_line(i) for i in video_data['title']]
    up_data = pd.read_csv(up_data_csv)
    up_data['up'] = [add_line(i) for i in up_data['up']]
    video_bar = (Bar(init_opts=opts.InitOpts(theme=ThemeType.ESSOS,width="1200px"),)
                  .add_xaxis(video_data['title'].tolist())
                  .add_yaxis('转发次数',video_data['share_data'].tolist())
                  .set_global_opts(title_opts=opts.TitleOpts(title='转发次数最多的视频',pos_left='center',pos_top='5%'),
                                    toolbox_opts=opts.TitleOpts(),
                                    legend_opts=opts.LegendOpts(is_show=False),
                                    xaxis_opts=opts.AxisOpts(axislabel_opts=opts.LabelOpts(rotate=0,font_size = 10),))
                 .set_series_opts(label_opts=opts.LabelOpts(is_show=False))
    )
    up_bar = (Bar(init_opts=opts.InitOpts(theme=ThemeType.ESSOS,width="1200px"))
                  .add_xaxis(up_data['up'].tolist())
                  .add_yaxis('转发次数',up_data['share_data'].tolist())
                  .set_global_opts(title_opts=opts.TitleOpts(title='转发次数最多的up主',pos_left='center',pos_top='5%'),
                                    toolbox_opts=opts.TitleOpts(),
                                    legend_opts=opts.LegendOpts(is_show=False),
                                    xaxis_opts=opts.AxisOpts(axislabel_opts=opts.LabelOpts(rotate=0,font_size = 10),))
              .set_series_opts(label_opts=opts.LabelOpts(is_show=False))
    )
    save_dir = os.path.join(base_dir,'share_data.html')
    page = Page(layout=Page.SimplePageLayout)
    page.add(video_bar,up_bar)
    page.render(save_dir)


def show_popular_subject(data_dir,base_dir):
    # 读入数据
    video_data_csv = os.path.join(data_dir,'top_popular_subject.csv')
    video_data = pd.read_csv(video_data_csv)
    save_dir = os.path.join(base_dir,'view_popular_subject.html')
    # 对长标题添加换行符
    video_data_short = [add_line(i,2) for i in video_data['tname']]
    # 绘制柱状图
    video_popular_bar = (Bar(init_opts=opts.InitOpts(theme=ThemeType.ESSOS,width='600px'),)
                  .add_xaxis(video_data_short)
                  .add_yaxis('上榜次数',video_data['popular_subject_times'].tolist())
                  .set_global_opts(title_opts=opts.TitleOpts(title='上榜次数前十的话题',pos_right='18%',pos_top='5%'),
                                    toolbox_opts=opts.TitleOpts(),
                                    legend_opts=opts.LegendOpts(is_show=False),
                                    xaxis_opts=opts.AxisOpts(axislabel_opts=opts.LabelOpts(rotate=0,font_size = 10),))
                  .set_series_opts(label_opts=opts.LabelOpts(is_show=False))
    )
    # 绘制富文本饼图
    video_popular_pie = (
        Pie(init_opts=opts.InitOpts(theme=ThemeType.ESSOS))
            .add(
            "",
            [list(z) for z in zip(video_data['tname'].tolist(), video_data['popular_subject_times'].tolist())],
            radius=["30%", "55%"],
            center=["25%", "50%"],
            label_opts=opts.LabelOpts(
                position="outside",
                formatter="{a|{a}}{abg|}\n{hr|}\n {b|{b}: }{c}  {per|{d}%}  ",
                background_color="#eee",
                border_color="#aaa",
                border_width=1,
                border_radius=3,
                rich={
                    "a": {"color": "#999", "lineHeight": 22, "align": "top"},
                    "abg": {
                        "backgroundColor": "#e3e3e3",
                        "width": "50%",
                        "align": "right",
                        "height": 22,
                        "borderRadius": [4, 4, 0, 0],
                    },
                    "hr": {
                        "borderColor": "#aaa",
                        "width": "50%",
                        "borderWidth": 0.5,
                        "height": 0,
                    },
                    "b": {"fontSize": 16, "lineHeight": 33},
                    "per": {
                        "color": "#eee",
                        "backgroundColor": "#334455",
                        "padding": [2, 3],
                        "borderRadius": 2,
                    },
                },
            ),
        )
            .set_global_opts(title_opts=opts.TitleOpts(title="上榜次数前十的话题占比",pos_left='20%',pos_top='5%'),\
            legend_opts=opts.LegendOpts(is_show=False,type_="scroll",orient='vertical',pos_top='10%',pos_right='50%'))
    )
    # 通过Grid将两个结果按照左右位置进行布局，渲染到html文件中
    grid = Grid(init_opts=opts.InitOpts(theme=ThemeType.ESSOS,width='1400'))
    grid.add(video_popular_bar,grid_opts=opts.GridOpts(pos_left="60%"))
    grid.add(video_popular_pie,grid_opts=opts.GridOpts(pos_left="50%"))
    grid.render(save_dir)

def show_popular_up(data_dir, base_dir):
    up_data_csv = os.path.join(data_dir, 'top_popular_up.csv')
    up_data = pd.read_csv(up_data_csv)
    save_dir = os.path.join(base_dir, 'view_popular_up.html')
    up_data_short = [add_line(i, 2) for i in up_data['up']]
    video_popular_bar = (Bar(init_opts=opts.InitOpts(theme=ThemeType.ESSOS, width='600px'), )
        .add_xaxis(up_data_short)
        .add_yaxis('上榜次数', up_data['popular_up_times'].tolist())
        .set_global_opts(
        title_opts=opts.TitleOpts(title='上榜次数前十的up主', pos_right='18%', pos_top='5%'),
        toolbox_opts=opts.TitleOpts(),
        legend_opts=opts.LegendOpts(is_show=False),
        xaxis_opts=opts.AxisOpts(axislabel_opts=opts.LabelOpts(rotate=0, font_size=10), ))
        .set_series_opts(label_opts=opts.LabelOpts(is_show=False))
    )
    video_popular_pie = (
        Pie(init_opts=opts.InitOpts(theme=ThemeType.ESSOS))
            .add(
            "",
            [list(z) for z in zip(up_data['up'].tolist(), up_data['popular_up_times'].tolist())],
            radius=["30%", "55%"],
            center=["25%", "50%"],
            label_opts=opts.LabelOpts(
                position="outside",
                formatter="{a|{a}}{abg|}\n{hr|}\n {b|{b}: }{c}  {per|{d}%}  ",
                background_color="#eee",
                border_color="#aaa",
                border_width=1,
                border_radius=3,
                rich={
                    "a": {"color": "#999", "lineHeight": 22, "align": "top"},
                    "abg": {
                        "backgroundColor": "#e3e3e3",
                        "width": "50%",
                        "align": "right",
                        "height": 22,
                        "borderRadius": [4, 4, 0, 0],
                    },
                    "hr": {
                        "borderColor": "#aaa",
                        "width": "50%",
                        "borderWidth": 0.5,
                        "height": 0,
                    },
                    "b": {"fontSize": 16, "lineHeight": 33},
                    "per": {
                        "color": "#eee",
                        "backgroundColor": "#334455",
                        "padding": [2, 3],
                        "borderRadius": 2,
                    },
                },
            ),
        )
            .set_global_opts(title_opts=opts.TitleOpts(title="上榜次数前十的up主占比", pos_left='20%', pos_top='5%'),
                             legend_opts=opts.LegendOpts(is_show=False, type_="scroll", orient='vertical',
                                                         pos_top='10%', pos_right='50%'))
    )
    grid = Grid(init_opts=opts.InitOpts(theme=ThemeType.ESSOS, width='1400'))
    grid.add(video_popular_bar, grid_opts=opts.GridOpts(pos_left="60%"))
    grid.add(video_popular_pie, grid_opts=opts.GridOpts(pos_left="50%"))
    grid.render(save_dir)

def show_popular(data_dir, base_dir):
    show_popular_up(data_dir, base_dir)
    show_popular_subject(data_dir, base_dir)

def show_statistic(base_dir,render_save_dir):
    video_coin_data = os.path.join(base_dir,'video_coin_data.csv')
    up_coin_data = os.path.join(rendered_data,'up_coin_data.csv')
    show_coin_data(video_coin_data, up_coin_data,render_save_dir)

    video_danmaku_data = os.path.join(base_dir,'video_danmaku_data.csv')
    up_danmaku_data = os.path.join(rendered_data,'up_danmaku_data.csv')
    show_danmaku_data(video_danmaku_data, up_danmaku_data,render_save_dir)

    video_favorite_data = os.path.join(base_dir,'video_favorite_data.csv')
    up_favorite_data = os.path.join(rendered_data,'up_favorite_data.csv')
    show_favorite_data(video_favorite_data, up_favorite_data,render_save_dir)

    video_like_data = os.path.join(base_dir,'video_like_data.csv')
    up_like_data = os.path.join(rendered_data,'up_like_data.csv')
    show_like_data(video_like_data, up_like_data,render_save_dir)

    video_reply_data = os.path.join(base_dir,'video_reply_data.csv')
    up_reply_data = os.path.join(rendered_data,'up_reply_data.csv')
    show_reply_data(video_reply_data, up_reply_data,render_save_dir)

    video_share_data = os.path.join(base_dir,'video_share_data.csv')
    up_share_data = os.path.join(rendered_data,'up_share_data.csv')
    show_share_data(video_share_data, up_share_data,render_save_dir)

    video_view_data = os.path.join(base_dir,'video_view_data.csv')
    up_view_data = os.path.join(rendered_data,'up_view_data.csv')
    show_view_data(video_view_data, up_view_data,render_save_dir)

def map_to_list(df):
    word = df['word'].tolist()
    count = df['count'].tolist()
    data_list = []
    for _word, _cnt in zip(word,count):
        data_list.append((_word,_cnt))
    return data_list

def show_word_clowd(base_dir,render_save_dir):
    save_dir = os.path.join(render_save_dir,'wordcloud.html')
    # 读取数据
    title_words = pd.read_csv(os.path.join(base_dir,'title_word.csv'))
    # 把数据转为list
    title_words_freq = map_to_list(title_words)
    # 绘制词云
    title_wc = (
    WordCloud(init_opts=opts.InitOpts(theme=ThemeType.ESSOS))
    .add(
        series_name="",
        shape='circle',
        data_pair=title_words_freq,            #数据来源
        word_size_range=[16, 66],    #字号大小范围设置
        textstyle_opts=opts.TextStyleOpts(
        color='#dcdada'
        ),
    )
    .set_global_opts(     #全局设置项
        title_opts=opts.TitleOpts(
            title="标题常见词", title_textstyle_opts=opts.TextStyleOpts(font_size=23),pos_right='center'
        ),
        tooltip_opts=opts.TooltipOpts(is_show=False),
    )
    .set_series_opts(
        areastyle_opts=opts.AreaStyleOpts(opacity = 0)
    )
    )

    desc_words = pd.read_csv(os.path.join(base_dir,'desc_word.csv'))
    desc_words_freq = map_to_list(desc_words)
    desc_wc = (
    WordCloud(init_opts=opts.InitOpts(theme=ThemeType.ESSOS))
    .add(
        series_name="设置系列名",
        shape='circle',
        data_pair=desc_words_freq,            #数据来源
        word_size_range=[16, 66],    #字号大小范围设置
        textstyle_opts=opts.TextStyleOpts(
        color='#dcdada'
        ),
    )
    .set_global_opts(     #全局设置项
        title_opts=opts.TitleOpts(
            title="视频简介常见词", title_textstyle_opts=opts.TextStyleOpts(font_size=23),pos_right='center'
        ),
        tooltip_opts=opts.TooltipOpts(is_show=False),
    )
    .set_series_opts(
        areastyle_opts=opts.AreaStyleOpts(opacity = 0)
    )
    )

    rcmd_reason_words = pd.read_csv(os.path.join(base_dir,'rcmd_reason_word.csv'))
    rcmd_reason_words_freq = map_to_list(rcmd_reason_words)
    rcmd_reason_wc = (
    WordCloud(init_opts=opts.InitOpts(theme=ThemeType.ESSOS))
    .add(
        series_name="设置系列名",
        shape='circle',
        data_pair=rcmd_reason_words_freq,            #数据来源
        word_size_range=[16, 66],    #字号大小范围设置
        textstyle_opts=opts.TextStyleOpts(
        color='#dcdada'
        ),

    )
    .set_global_opts(     #全局设置项
        title_opts=opts.TitleOpts(
            title="推荐理由常见词", title_textstyle_opts=opts.TextStyleOpts(font_size=23),pos_right='center'
        ),
        tooltip_opts=opts.TooltipOpts(is_show=False),
    )
    .set_series_opts(
        areastyle_opts=opts.AreaStyleOpts(opacity = 0)
    )
    )
    # 通过Page()，把三个可视化结果按照上中下顺序布局，渲染成html文件
    page = Page(layout=Page.SimplePageLayout)
    page.add(title_wc,desc_wc,rcmd_reason_wc)
    page.render(save_dir)


def show_Mllib(base_dir, render_save_dir):
    save_dir = os.path.join(render_save_dir, 'MLlib.html')
    # 读取分类器的性能指标
    comparison_csv = pd.read_csv(os.path.join(base_dir, 'comparison.csv'))
    # 保留4位有效数据
    comparison_csv = comparison_csv.round(4)
    # 对名字较长的分类器添加换行符
    comparison_csv['classifier'] = [add_line_eng(i) for i in comparison_csv['classifier']]
    # 绘制双柱状图
    comparison_bar = (Bar(init_opts=opts.InitOpts(theme=ThemeType.ESSOS, width="1200px"))
                      .add_xaxis(comparison_csv['classifier'].tolist())
                      .add_yaxis('Acc', comparison_csv['Acc'].tolist())
                      .add_yaxis('Auc', comparison_csv['Auc'].tolist())
                      .set_global_opts(
        title_opts=opts.TitleOpts(title='不同分类器的性能比较', pos_left='center', pos_top='1%'),
        toolbox_opts=opts.TitleOpts(),
        legend_opts=opts.LegendOpts(pos_left='10%', pos_top='6%'),
        xaxis_opts=opts.AxisOpts(axislabel_opts=opts.LabelOpts(rotate=0, font_size=10), ),
        )
        .set_series_opts(label_opts=opts.LabelOpts(is_show=False))
                      )

    # 读取相关矩阵
    cor_mat_csv = pd.read_csv(os.path.join(base_dir, 'cor_matrix.csv'))
    # 保留4位小数
    cor_mat_csv = cor_mat_csv.round(4)
    cor_mat_csv = cor_mat_csv.values
    # 对数据数据为[i,j,value]的格式，表示变量i和变量j之间的相关性为value
    variable_list = ['view', 'danmaku', 'reply', 'favorite', 'coin', 'share', 'like']
    data = [[i, j, cor_mat_csv[i][j]] for i in range(len(variable_list)) for j in range(len(variable_list))]
    # 绘制热力图
    heatmap = (
        HeatMap(init_opts=opts.InitOpts(theme=ThemeType.ESSOS, width="1200px"))
        .add_xaxis(variable_list)
        .add_yaxis("", variable_list, data)
        .set_global_opts(
            title_opts=opts.TitleOpts(title="自变量相关性", pos_right='center', pos_top='2%'),
            visualmap_opts=opts.VisualMapOpts(max_=1, pos_right='5%', pos_bottom='20%'),
            xaxis_opts=opts.AxisOpts(name="Features", axislabel_opts=opts.LabelOpts(font_size=10), ),
            yaxis_opts=opts.AxisOpts(name="Features", axislabel_opts=opts.LabelOpts(font_size=10)),
        )
        .set_series_opts(label_opts=opts.LabelOpts(is_show=False))
    )
    # 通过Page()类，将热力图和双柱状图按照上下的排列顺序渲染成html文件
    page = Page(layout=Page.SimplePageLayout)
    page.add(heatmap, comparison_bar)
    page.render(save_dir)


if __name__ == '__main__':
    # 分析结果保存路径
    rendered_data = './static'
    # 项目保存路径
    render_html = './html'
    os.makedirs(render_html,exist_ok=True)
    # 数据可视化
    show_statistic(rendered_data,render_html)
    # 上榜视频与up主信息可视化
    show_popular(rendered_data,render_html)
    # 词频可视化
    show_word_clowd(rendered_data,render_html)
    # 机器学习组件可视化
    show_Mllib(rendered_data,render_html)