import os
from glob import glob
import json
import pandas as pd
from pandas import json_normalize
pd.set_option('display.max_rows', None)  # 显示所有行
pd.set_option('display.max_columns', None)  # 显示所有列
pd.set_option('display.width', None)  # 不折叠单元格
pd.set_option('display.max_colwidth', None)  # 显示完整的单元格内容

# 对每一个json提取有用的数据
def select_data(json_fpath):
    # 选择有价值的数据：
    # up: 视频发布者的名字
    # time: 所属的期数
    # title: 视频标题
    # desc: 视频简介
    # view: 观看人数
    # danmaku: 弹幕数
    # reply: 回复数
    # favorite: 收藏人数
    # coin: 投币人数
    # share: 分享次数
    # like: 点赞次数
    # dislike: 不喜欢次数
    # rcmd_reason: 推荐理由
    # tname: 视频分区
    # his_rank: 历史排名
    key = ['up', 'time', 'title', 'desc', 'view', 'danmaku', 'reply', 'favorite', \
           'coin', 'share', 'like', 'dislike','rcmd_reason', 'tname', 'his_rank']
    _df = pd.DataFrame(columns=key)
    _week = pd.read_json(json_fpath)

    # 获取每个必看视频基础的信息
    _base_info = pd.DataFrame(_week['data']['list'])
    _df[['tname', 'title', 'desc', 'rcmd_reason']] = _base_info[['tname', 'title', 'desc', 'rcmd_reason']]
    # 获取up主的信息
    _owner = _base_info['owner'].values.tolist()
    _owner = pd.DataFrame(_owner)
    _df['up'] = _owner['name']
    # 获取视频的播放信息
    _stat = _base_info['stat'].values.tolist()
    _stat = pd.DataFrame(_stat)
    _df[['view', 'danmaku', 'reply', 'favorite', 'coin', 'share', 'his_rank', 'like', 'dislike']] = \
        _stat[['view', 'danmaku', 'reply', 'favorite', 'coin', 'share', 'his_rank', 'like', 'dislike']]
    _df['time'] = _week['data']['config']['name']
    return _df


def clean_data(dataframe):
    # 删除掉包含有空值的数据行
    dataframe.dropna(how='any', axis=0, inplace=True)
    # 删除掉重复的数据，其中当视频标题与作者名称相同认为数据重复
    dataframe = dataframe.drop_duplicates(subset=['title', 'up'])
    # 删除掉异常的数据，认为观看数、弹幕数、回复、喜欢、投币、分享、收藏数小于或等于0，不喜欢的人数超过0则认为数据异常
    incorrect_df = dataframe.loc[(dataframe['view'] <= 0) \
                                 | (dataframe['danmaku'] <= 0) \
                                 | (dataframe['reply'] <= 0) \
                                 | (dataframe['favorite'] <= 0) \
                                 | (dataframe['coin'] <= 0) \
                                 | (dataframe['share'] <= 0) \
                                 | (dataframe['like'] <= 0) \
                                 | (dataframe['dislike'] > 0) \
                                 | (dataframe['his_rank'] <= 0)]
    # 丢弃异常数据
    dataframe = dataframe.drop(incorrect_df.index)
    # dislike数据无意义，丢弃
    dataframe = dataframe.drop(columns=['dislike'])

    # 处理描述为空的数据，使用该视频的标题进行填充
    desc_none = dataframe.loc[(dataframe['desc'] == '') | (dataframe['desc'] == '-')]
    for i in range(desc_none.shape[0]):
        dataframe.loc[desc_none.index[i], 'desc'] = dataframe.loc[desc_none.index[i], 'title']
    # 处理推荐理由为空的数据，使用该视频的标题进行填充
    rcmd_reason_none = dataframe.loc[(dataframe['rcmd_reason'] == '') | (dataframe['rcmd_reason'] == '-')]
    for i in range(rcmd_reason_none.shape[0]):
        dataframe.loc[rcmd_reason_none.index[i], 'rcmd_reason'] = dataframe.loc[rcmd_reason_none.index[i], 'title']
    # 对长文本进行处理
    dataframe['title'] = dataframe['title'].map(lambda x: x.replace(",", " "))
    dataframe['desc'] = dataframe['desc'].map(lambda x: x.replace(",", " "))
    dataframe['rcmd_reason'] = dataframe['rcmd_reason'].map(lambda x: x.replace(",", " "))
    dataframe['title'] = dataframe['title'].map(lambda x: x.replace(";", " "))
    dataframe['desc'] = dataframe['desc'].map(lambda x: x.replace(";", " "))
    dataframe['rcmd_reason'] = dataframe['rcmd_reason'].map(lambda x: x.replace(";", " "))
    dataframe['title'] = dataframe['title'].map(lambda x: x.replace("\n"," "))
    dataframe['desc'] = dataframe['desc'].map(lambda x: x.replace("\n"," "))
    dataframe['rcmd_reason'] = dataframe['rcmd_reason'].map(lambda x: x.replace("\n"," "))
    dataframe['title'] = dataframe['title'].map(lambda x: x.replace("\r"," "))
    dataframe['desc'] = dataframe['desc'].map(lambda x: x.replace("\r"," "))
    dataframe['rcmd_reason'] = dataframe['rcmd_reason'].map(lambda x: x.replace("\r"," "))
    dataframe['title'] = dataframe['title'].map(lambda x: x.replace("\t"," "))
    dataframe['desc'] = dataframe['desc'].map(lambda x: x.replace("\t"," "))
    dataframe['rcmd_reason'] = dataframe['rcmd_reason'].map(lambda x: x.replace("\t"," "))
    return dataframe


# 合并所有的json
def merge_data(json_data, save_path):
    key = ['up', 'time', 'title', 'desc', 'view', 'danmaku', 'reply', 'favorite', \
           'coin', 'share', 'like', 'dislike','rcmd_reason', 'tname', 'his_rank']
    df = pd.DataFrame(columns=key)
    # 依次处理每周的数据并进行合并
    for each_json in json_data:
        _df = select_data(each_json)
        df = pd.concat([df, _df], ignore_index=True)
    # 进行数据处理
    df = clean_data(df)
    # 保存为txt文件
    df.to_csv(save_path, header=None, index=None, sep='\t', mode='w')

if __name__ == '__main__':
    # 读取所有的json数据
    json_data = glob(os.path.join('./data', '*.json'))
    #最后的数据保存路径
    save_dir = 'bilibili_week.txt'
    # 进行数据合并并存储为txt文件
    merge_data(json_data, save_dir)
