# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html

from  scrapy import Item,Field

class postsItem(Item):
    #主题id
    thread_id = Field()
    #不同楼层帖子id
    post_id = Field()
    #楼层发帖内容
    content = Field()
    #不同楼层用户id
    author_id = Field()
    #作者姓名
    author_name = Field()
    #评论信息
    comment_content = Field()
    #评论数目
    comment_num = Field()
    #楼层号
    floor_no = Field()
    #时间
    post_date = Field()
    #回复数目
    commet_num = Field()
    #使用设备
    shebei = Field()
    # user_home = Field()
    #评论信息的json格式
    # comment_info_json = Field()


class userInfo(Item):
    user_id = Field()
    user_name = Field()



